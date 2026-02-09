#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Bien'ici - Locations
Stratégie: subdivision adaptative pour contourner la limite de 2500 résultats.

L'API Bien'ici (Elasticsearch) plafonne à ~2500 résultats par requête paginée.
Solution: découper les requêtes par fourchettes de prix de plus en plus fines
jusqu'à ce que chaque tranche contienne < 2400 résultats.
"""

import requests
import time
import os
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Optional, Tuple
import json
import random

load_dotenv()

# =============================================================
# CONFIGURATION
# =============================================================

MAX_RESULTS_WINDOW = 2400   # Limite Elasticsearch (on garde une marge)
MIN_PRICE_SLICE = 10        # Plus petite tranche de prix (€) avant d'arrêter la subdivision
MAX_SUBDIVISION_DEPTH = 10  # Profondeur max de récursion


class BieniciScraper:
    def __init__(self):
        # MongoDB
        self.mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        self.db_name = os.getenv('MONGODB_DATABASE', 'bienici')
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.collection = self.db['locations']

        # Scraper config
        self.api_url = os.getenv('BIENICI_API_URL', 'https://www.bienici.com/realEstateAds.json')
        self.delay = int(os.getenv('DELAY_BETWEEN_REQUESTS', 2))
        self.max_pages = int(os.getenv('MAX_PAGES', 100))
        self.items_per_page = int(os.getenv('ITEMS_PER_PAGE', 100))

        # Fourchettes de prix initiales (seront subdivisées si nécessaire)
        self.initial_price_ranges = [
            (0, 400),
            (400, 600),
            (600, 800),
            (800, 1000),
            (1000, 1200),
            (1200, 1500),
            (1500, 2000),
            (2000, 2500),
            (2500, 3500),
            (3500, 5000),
            (5000, 10000),
            (10000, 50000),
        ]

        # Stats
        self.stats = {
            'total_scraped': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0,
            'api_calls': 0,
            'subdivisions': 0,
        }

        self.create_indexes()

    # ---------------------------------------------------------
    # INDEX
    # ---------------------------------------------------------
    def create_indexes(self):
        print("📊 Index MongoDB...")
        try:
            for idx in list(self.collection.index_information().keys()):
                if idx != '_id_':
                    try:
                        self.collection.drop_index(idx)
                    except Exception:
                        pass
        except Exception:
            pass

        self.collection.create_index([('id', ASCENDING)], unique=True, name='id_unique')
        self.collection.create_index([('city', ASCENDING)])
        self.collection.create_index([('postalCode', ASCENDING)])
        self.collection.create_index([('propertyType', ASCENDING)])
        self.collection.create_index([('price', ASCENDING)])
        self.collection.create_index([
            ('city', ASCENDING),
            ('propertyType', ASCENDING),
            ('price', ASCENDING),
        ])
        print("  ✅ Index créés\n")

    # ---------------------------------------------------------
    # API CALL (avec retry + jitter)
    # ---------------------------------------------------------
    def fetch(self, filters: Dict, retries: int = 3) -> Optional[Dict]:
        """Appel API avec retry exponentiel."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'fr-FR,fr;q=0.9',
        }
        params = {'filters': json.dumps(filters)}

        for attempt in range(retries):
            try:
                self.stats['api_calls'] += 1
                resp = requests.get(self.api_url, params=params, headers=headers, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                wait = (2 ** attempt) + random.uniform(0, 1)
                print(f"      ⚠️  Erreur API (tentative {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    print(f"      ⏳ Retry dans {wait:.1f}s...")
                    time.sleep(wait)
                else:
                    self.stats['errors'] += 1
                    return None

    # ---------------------------------------------------------
    # PROBE: compter combien de résultats dans une tranche
    # ---------------------------------------------------------
    def probe_total(self, filter_type: str, property_type: str, price_min: int, price_max: int) -> int:
        """
        Fait un appel léger (size=1) pour connaître le total de résultats
        d'une tranche de prix, sans télécharger les données.
        """
        filters = {
            "size": 1,
            "from": 0,
            "filterType": filter_type,
            "propertyType": [property_type],
            "sortBy": "publicationDate",
            "sortOrder": "desc",
            "onTheMarket": [True],
            "minPrice": price_min,
            "maxPrice": price_max,
        }
        resp = self.fetch(filters)
        if resp:
            return resp.get('total', 0)
        return 0

    # ---------------------------------------------------------
    # SUBDIVISION ADAPTATIVE
    # ---------------------------------------------------------
    def build_slices(
        self,
        filter_type: str,
        property_type: str,
        price_min: int,
        price_max: int,
        depth: int = 0,
    ) -> List[Tuple[int, int]]:
        """
        Retourne une liste de tranches (min, max) dont chacune contient
        <= MAX_RESULTS_WINDOW résultats.

        Si une tranche dépasse la limite, on la coupe en deux et on
        ré-évalue récursivement.
        """
        total = self.probe_total(filter_type, property_type, price_min, price_max)

        # Tranche OK : on la garde telle quelle
        if total <= MAX_RESULTS_WINDOW:
            if total > 0:
                return [(price_min, price_max)]
            return []  # Aucune annonce dans cette tranche

        # Trop de résultats → subdiviser
        if depth >= MAX_SUBDIVISION_DEPTH or (price_max - price_min) <= MIN_PRICE_SLICE:
            # On ne peut plus subdiviser, on prend ce qu'on peut (2400 max)
            print(f"      ⚠️  Tranche {price_min}-{price_max}€ trop dense "
                  f"({total} annonces), limite de subdivision atteinte")
            return [(price_min, price_max)]

        self.stats['subdivisions'] += 1
        mid = (price_min + price_max) // 2
        print(f"      🔀 Subdivision: {price_min}-{price_max}€ ({total} annonces) "
              f"→ [{price_min}-{mid}] + [{mid}-{price_max}]")

        left = self.build_slices(filter_type, property_type, price_min, mid, depth + 1)
        right = self.build_slices(filter_type, property_type, mid, price_max, depth + 1)

        time.sleep(0.5)  # Petit délai entre les probes
        return left + right

    # ---------------------------------------------------------
    # SCRAPE UNE TRANCHE
    # ---------------------------------------------------------
    def scrape_slice(self, filter_type: str, property_type: str, price_min: int, price_max: int):
        """Scraper toutes les pages d'une tranche de prix (garantie < 2400 résultats)."""
        from_index = 0
        page_num = 1

        while page_num <= self.max_pages:
            filters = {
                "size": self.items_per_page,
                "from": from_index,
                "filterType": filter_type,
                "propertyType": [property_type],
                "page": page_num,
                "sortBy": "publicationDate",
                "sortOrder": "desc",
                "onTheMarket": [True],
                "minPrice": price_min,
                "maxPrice": price_max,
            }

            resp = self.fetch(filters)
            if not resp:
                break

            annonces = resp.get('realEstateAds', [])
            total = resp.get('total', 0)

            if not annonces:
                break

            result = self.save_annonces(annonces)

            self.stats['total_scraped'] += len(annonces)
            self.stats['inserted'] += result['inserted']
            self.stats['updated'] += result['updated']
            self.stats['skipped'] += result['skipped']

            print(f"        📄 p{page_num}: {len(annonces)} annonces "
                  f"(🆕{result['inserted']} 🔄{result['updated']}) "
                  f"- {from_index + len(annonces)}/{total}")

            from_index += len(annonces)
            if from_index >= total:
                break

            page_num += 1
            time.sleep(self.delay)

    # ---------------------------------------------------------
    # PIPELINE PRINCIPAL
    # ---------------------------------------------------------
    def scrape_property_type(self, filter_type: str, property_type: str):
        """Scraper un type de bien avec subdivision adaptative."""
        print(f"\n  📦 {property_type.upper()}")
        print(f"  {'─'*50}")

        all_slices = []
        for price_min, price_max in self.initial_price_ranges:
            slices = self.build_slices(filter_type, property_type, price_min, price_max)
            all_slices.extend(slices)

        print(f"\n  📋 {len(all_slices)} tranches à scraper "
              f"(après {self.stats['subdivisions']} subdivisions)\n")

        for i, (p_min, p_max) in enumerate(all_slices, 1):
            total_est = self.probe_total(filter_type, property_type, p_min, p_max)
            print(f"    💵 [{i}/{len(all_slices)}] {p_min}-{p_max}€ "
                  f"(~{total_est} annonces)")

            if total_est == 0:
                print(f"        ⏭️  Vide, on passe")
                continue

            self.scrape_slice(filter_type, property_type, p_min, p_max)

            # Log de progression tous les 10 tranches
            if i % 10 == 0:
                total_db = self.collection.count_documents({})
                print(f"\n    📊 Progression: {i}/{len(all_slices)} tranches | "
                      f"DB: {total_db} | API calls: {self.stats['api_calls']}\n")

    def scrape_all(self):
        start_time = time.time()

        print("\n" + "=" * 60)
        print("🏠 SCRAPER BIEN'ICI - LOCATIONS (Subdivision Adaptative)")
        print("=" * 60)

        for ptype in ['flat', 'house']:
            self.scrape_property_type('rent', ptype)

        self.print_stats(time.time() - start_time)

    # ---------------------------------------------------------
    # SAVE
    # ---------------------------------------------------------
    def prepare_annonce(self, data: Dict) -> Dict:
        prepared = {
            'id': data.get('id'),
            'reference': data.get('reference'),
            'source': 'bienici',
            'title': data.get('title'),
            'description': data.get('description'),
            'city': data.get('city'),
            'postalCode': data.get('postalCode'),
            'district': data.get('district'),
            'country': data.get('country'),
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude'),
            'price': data.get('price'),
            'rentalPrice': data.get('rentalPrice'),
            'pricePerSquareMeter': data.get('pricePerSquareMeter'),
            'priceHasDecreased': data.get('priceHasDecreased'),
            'charges': data.get('charges'),
            'chargesIncluded': data.get('chargesIncluded'),
            'agencyRentalFee': data.get('agencyRentalFee'),
            'safetyDeposit': data.get('safetyDeposit'),
            'tenantFees': data.get('tenantFees'),
            'propertyType': data.get('propertyType'),
            'surfaceArea': data.get('surfaceArea'),
            'landSurfaceArea': data.get('landSurfaceArea'),
            'roomsQuantity': data.get('roomsQuantity'),
            'bedroomsQuantity': data.get('bedroomsQuantity'),
            'bathroomsQuantity': data.get('bathroomsQuantity'),
            'showerRoomsQuantity': data.get('showerRoomsQuantity'),
            'toiletQuantity': data.get('toiletQuantity'),
            'floor': data.get('floor'),
            'floorQuantity': data.get('floorQuantity'),
            'newProperty': data.get('newProperty'),
            'yearOfConstruction': data.get('yearOfConstruction'),
            'condition': data.get('condition'),
            'isFurnished': data.get('isFurnished'),
            'isStudio': data.get('isStudio'),
            'publicationDate': data.get('publicationDate'),
            'modificationDate': data.get('modificationDate'),
            'availableDate': data.get('availableDate'),
            'adType': data.get('adType'),
            'transactionType': data.get('transactionType'),
            'adTypeFR': data.get('adTypeFR'),
            'accountType': data.get('accountType'),
            'adCreatedByPro': data.get('adCreatedByPro'),
            'hasBalcony': data.get('hasBalcony'),
            'hasTerrace': data.get('hasTerrace'),
            'hasGarden': data.get('hasGarden'),
            'hasPool': data.get('hasPool'),
            'hasCellar': data.get('hasCellar'),
            'hasGarage': data.get('hasGarage'),
            'hasParking': data.get('hasParking'),
            'hasSeparateToilet': data.get('hasSeparateToilet'),
            'hasIntercom': data.get('hasIntercom'),
            'hasElevator': data.get('hasElevator'),
            'hasFireplace': data.get('hasFireplace'),
            'hasAirConditioning': data.get('hasAirConditioning'),
            'hasDisabledAccess': data.get('hasDisabledAccess'),
            'energyClassification': data.get('energyClassification'),
            'energyValue': data.get('energyValue'),
            'greenhouseGazClassification': data.get('greenhouseGazClassification'),
            'greenhouseGazValue': data.get('greenhouseGazValue'),
            'heating': data.get('heating'),
            'heatingType': data.get('heatingType'),
            'exposition': data.get('exposition'),
            'parkingPlacesQuantity': data.get('parkingPlacesQuantity'),
            'garagesQuantity': data.get('garagesQuantity'),
            'photos': data.get('photos', []),
            'photosCount': data.get('photosCount'),
            'virtualTour': data.get('virtualTour'),
            'agency': data.get('agency'),
            'agencyId': data.get('agencyId'),
            'agencyName': data.get('agencyName'),
            'agencyPhone': data.get('agencyPhone'),
            'contactPhone': data.get('contactPhone'),
            'diagnostics': data.get('diagnostics', []),
            'status': data.get('status'),
            'tags': data.get('tags', []),
            'isExclusive': data.get('isExclusive'),
            'isNew': data.get('isNew'),
            'scraped_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
        }
        return {k: v for k, v in prepared.items() if v is not None}

    def save_annonces(self, annonces: List[Dict]) -> Dict:
        if not annonces:
            return {'inserted': 0, 'updated': 0, 'skipped': 0}

        inserted = updated = skipped = 0

        for annonce in annonces:
            try:
                prepared = self.prepare_annonce(annonce)
                aid = prepared.get('id')
                if not aid:
                    skipped += 1
                    continue

                existing = self.collection.find_one({'id': aid})
                if existing:
                    self.collection.update_one({'id': aid}, {'$set': prepared})
                    updated += 1
                else:
                    prepared['created_at'] = datetime.utcnow()
                    self.collection.insert_one(prepared)
                    inserted += 1
            except DuplicateKeyError:
                skipped += 1
            except Exception as e:
                print(f"        ⚠️  Erreur save: {str(e)[:80]}")
                skipped += 1

        return {'inserted': inserted, 'updated': updated, 'skipped': skipped}

    # ---------------------------------------------------------
    # STATS
    # ---------------------------------------------------------
    def print_stats(self, duration: float):
        total_db = self.collection.count_documents({})
        print("\n" + "=" * 60)
        print("📊 STATISTIQUES FINALES")
        print("=" * 60)
        print(f"  Total scrapé:      {self.stats['total_scraped']}")
        print(f"  Nouvelles:         {self.stats['inserted']}")
        print(f"  Mises à jour:      {self.stats['updated']}")
        print(f"  Ignorées:          {self.stats['skipped']}")
        print(f"  Erreurs:           {self.stats['errors']}")
        print(f"  Appels API:        {self.stats['api_calls']}")
        print(f"  Subdivisions:      {self.stats['subdivisions']}")
        print(f"  Durée:             {duration:.0f}s ({duration/60:.1f}min)")
        print(f"  Total en DB:       {total_db}")
        print("=" * 60 + "\n")

    def close(self):
        self.client.close()


# =============================================================
# MAIN
# =============================================================
def main():
    scraper = BieniciScraper()
    try:
        scraper.scrape_all()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrompu")
        scraper.print_stats(0)
    except Exception as e:
        print(f"\n\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
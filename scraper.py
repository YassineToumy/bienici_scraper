#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import os
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Optional
import json

load_dotenv()

class BieniciScraper:
    def __init__(self):
        # Configuration MongoDB
        self.mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        self.db_name = os.getenv('MONGODB_DATABASE', 'bienici_scraper')
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.collection = self.db['annonces']
        
        # Configuration Scraper
        self.api_url = os.getenv('BIENICI_API_URL', 'https://www.bienici.com/realEstateAds.json')
        self.delay = int(os.getenv('DELAY_BETWEEN_REQUESTS', 2))
        self.max_pages = int(os.getenv('MAX_PAGES', 100))
        self.items_per_page = int(os.getenv('ITEMS_PER_PAGE', 100))
        
        # Départements français (métropole)
        self.departments = [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "21",
            "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B",
            "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
            "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
            "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
            "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
            "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
            "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
            "90", "91", "92", "93", "94", "95"
        ]
        
        # Statistiques
        self.stats = {
            'total_scraped': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        # Créer les index
        self.create_indexes()
        
    def create_indexes(self):
        """Créer les index MongoDB pour optimiser les recherches"""
        print("📊 Gestion des index MongoDB...")
        
        # Supprimer tous les anciens index (sauf _id_)
        try:
            existing_indexes = list(self.collection.index_information().keys())
            for idx in existing_indexes:
                if idx != '_id_':
                    try:
                        self.collection.drop_index(idx)
                        print(f"  🗑️  Index '{idx}' supprimé")
                    except:
                        pass
        except Exception as e:
            print(f"  ℹ️  Nettoyage index: {e}")
        
        # Créer UN SEUL index unique sur 'id'
        self.collection.create_index(
            [('id', ASCENDING)], 
            unique=True,
            name='id_unique'
        )
        print("  ✅ Index unique 'id' créé")
        
        # Index pour les recherches courantes (NON uniques)
        self.collection.create_index([('city', ASCENDING)])
        self.collection.create_index([('postalCode', ASCENDING)])
        self.collection.create_index([('propertyType', ASCENDING)])
        self.collection.create_index([('price', ASCENDING)])
        self.collection.create_index([('adType', ASCENDING)])
        self.collection.create_index([('publicationDate', ASCENDING)])
        
        # Index composés
        self.collection.create_index([
            ('city', ASCENDING),
            ('propertyType', ASCENDING),
            ('price', ASCENDING)
        ])
        
        print("  ✅ Index de recherche créés\n")
    
    def fetch_annonces(self, filters: Dict) -> Optional[Dict]:
        """Récupérer les annonces depuis l'API Bienici"""
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'fr-FR,fr;q=0.9',
        }
        
        params = {
            'filters': json.dumps(filters)
        }
        
        try:
            response = requests.get(
                self.api_url,
                params=params,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"\n❌ Erreur requête API: {e}")
            self.stats['errors'] += 1
            return None
    
    def prepare_annonce(self, data: Dict) -> Dict:
        """Préparer et nettoyer les données d'une annonce"""
        prepared = {
            # === IDENTIFICATION ===
            'id': data.get('id'),
            'reference': data.get('reference'),
            'source': 'bienici',
            'title': data.get('title'),
            'description': data.get('description'),
            
            # === LOCALISATION ===
            'city': data.get('city'),
            'postalCode': data.get('postalCode'),
            'district': data.get('district'),
            'country': data.get('country'),
            'id_polygone': data.get('id_polygone'),
            'location': data.get('location'),
            'insee_code': data.get('insee_code'),
            'code_insee': data.get('code_insee'),
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude'),
            
            # === PRIX ===
            'price': data.get('price'),
            'rentalPrice': data.get('rentalPrice'),
            'pricePerSquareMeter': data.get('pricePerSquareMeter'),
            'priceHasDecreased': data.get('priceHasDecreased'),
            'priceEvolution': data.get('priceEvolution'),
            
            # === CHARGES ===
            'agencyRentalFee': data.get('agencyRentalFee'),
            'safetyDeposit': data.get('safetyDeposit'),
            'charges': data.get('charges'),
            'chargesIncluded': data.get('chargesIncluded'),
            'tenantFees': data.get('tenantFees'),
            'tenantFeesPercentage': data.get('tenantFeesPercentage'),
            
            # === CARACTÉRISTIQUES ===
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
            
            # === ÉTAT ===
            'newProperty': data.get('newProperty'),
            'yearOfConstruction': data.get('yearOfConstruction'),
            'condition': data.get('condition'),
            'isUnderCompromise': data.get('isUnderCompromise'),
            'isFurnished': data.get('isFurnished'),
            'isStudio': data.get('isStudio'),
            
            # === DATES ===
            'publicationDate': data.get('publicationDate'),
            'modificationDate': data.get('modificationDate'),
            'availableDate': data.get('availableDate'),
            'closingDate': data.get('closingDate'),
            'expirationDate': data.get('expirationDate'),
            
            # === TYPE ===
            'adType': data.get('adType'),
            'transactionType': data.get('transactionType'),
            'adTypeFR': data.get('adTypeFR'),
            'accountType': data.get('accountType'),
            'adCreatedByPro': data.get('adCreatedByPro'),
            
            # === ÉQUIPEMENTS EXTÉRIEURS ===
            'hasBalcony': data.get('hasBalcony'),
            'hasTerrace': data.get('hasTerrace'),
            'hasGarden': data.get('hasGarden'),
            'hasPool': data.get('hasPool'),
            'hasCellar': data.get('hasCellar'),
            'hasGarage': data.get('hasGarage'),
            'hasParking': data.get('hasParking'),
            
            # === ÉQUIPEMENTS INTÉRIEURS ===
            'hasSeparateToilet': data.get('hasSeparateToilet'),
            'hasIntercom': data.get('hasIntercom'),
            'hasElevator': data.get('hasElevator'),
            'hasConservatory': data.get('hasConservatory'),
            'hasTwoWheelersRoom': data.get('hasTwoWheelersRoom'),
            'hasFireplace': data.get('hasFireplace'),
            'hasAirConditioning': data.get('hasAirConditioning'),
            'hasDisabledAccess': data.get('hasDisabledAccess'),
            
            # === DPE ===
            'energyClassification': data.get('energyClassification'),
            'energyValue': data.get('energyValue'),
            'greenhouseGazClassification': data.get('greenhouseGazClassification'),
            'greenhouseGazValue': data.get('greenhouseGazValue'),
            'heating': data.get('heating'),
            'heatingType': data.get('heatingType'),
            'exposition': data.get('exposition'),
            
            # === PARKING ===
            'parkingPlacesQuantity': data.get('parkingPlacesQuantity'),
            'garagesQuantity': data.get('garagesQuantity'),
            'boxQuantity': data.get('boxQuantity'),
            
            # === ÉQUIPEMENTS PROFESSIONNELS ===
            'hasCafeteria': data.get('hasCafeteria'),
            'hasCleaningService': data.get('hasCleaningService'),
            'hasConvenienceALS': data.get('hasConvenienceALS'),
            'hasConvenienceLunch': data.get('hasConvenienceLunch'),
            'hasCopyMachine': data.get('hasCopyMachine'),
            'hasFreeInternet': data.get('hasFreeInternet'),
            'hasFreeOpticFiberInternet': data.get('hasFreeOpticFiberInternet'),
            'hasLinenService': data.get('hasLinenService'),
            'hasManager': data.get('hasManager'),
            'hasMeetingRoom': data.get('hasMeetingRoom'),
            'hasPhonePoint': data.get('hasPhonePoint'),
            'hasTelevision': data.get('hasTelevision'),
            'hasWashingMachine': data.get('hasWashingMachine'),
            
            # === MÉDIAS ===
            'photos': data.get('photos', []),
            'photosCount': data.get('photosCount'),
            'virtualTour': data.get('virtualTour'),
            'videoUrl': data.get('videoUrl'),
            
            # === AGENCE ===
            'agency': data.get('agency'),
            'agencyId': data.get('agencyId'),
            'agencyName': data.get('agencyName'),
            'agencyLogo': data.get('agencyLogo'),
            'agencyPhone': data.get('agencyPhone'),
            
            # === CONTACT ===
            'contactPhone': data.get('contactPhone'),
            'contactEmail': data.get('contactEmail'),
            'showContactForm': data.get('showContactForm'),
            
            # === DIAGNOSTICS ===
            'diagnostics': data.get('diagnostics', []),
            
            # === DIVERS ===
            'status': data.get('status'),
            'tags': data.get('tags', []),
            'isFavorite': data.get('isFavorite'),
            'isExclusive': data.get('isExclusive'),
            'isNew': data.get('isNew'),
            'isPremium': data.get('isPremium'),
            'isUrgent': data.get('isUrgent'),
            'viewsCount': data.get('viewsCount'),
            'contactsCount': data.get('contactsCount'),
            
            # === MÉTADONNÉES ===
            'scraped_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
        }
        
        # Supprimer les None
        return {k: v for k, v in prepared.items() if v is not None}
    
    def save_annonces(self, annonces: List[Dict]) -> Dict:
        """
        Sauvegarder les annonces une par une pour gérer les doublons
        """
        if not annonces:
            return {'inserted': 0, 'updated': 0, 'skipped': 0}
        
        inserted = 0
        updated = 0
        skipped = 0
        
        for annonce in annonces:
            try:
                prepared = self.prepare_annonce(annonce)
                annonce_id = prepared.get('id')
                
                if not annonce_id:
                    skipped += 1
                    continue
                
                # Vérifier si l'annonce existe déjà
                existing = self.collection.find_one({'id': annonce_id})
                
                if existing:
                    # Mise à jour
                    self.collection.update_one(
                        {'id': annonce_id},
                        {'$set': prepared}
                    )
                    updated += 1
                else:
                    # Insertion
                    prepared['created_at'] = datetime.utcnow()
                    self.collection.insert_one(prepared)
                    inserted += 1
                    
            except DuplicateKeyError:
                # Doublon détecté (race condition possible)
                skipped += 1
            except Exception as e:
                print(f"\n  ⚠️  Erreur annonce {annonce.get('id')}: {str(e)[:100]}")
                skipped += 1
        
        return {
            'inserted': inserted,
            'updated': updated,
            'skipped': skipped
        }
    
    def scrape_with_department_filter(self, filter_type: str, property_type: str, department: str):
        """Scraper un département spécifique"""
        
        from_index = 0
        page_num = 1
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        while page_num <= self.max_pages:
            # Limite de l'API: 2400 par filtre
            if from_index >= 2400:
                print(f"      ⚠️  Limite API département {department}")
                break
            
            filters = {
                "size": self.items_per_page,
                "from": from_index,
                "filterType": filter_type,
                "propertyType": [property_type],
                "page": page_num,
                "sortBy": "publicationDate",
                "sortOrder": "desc",
                "onTheMarket": [True],
                "zoneIdsByTypes": {
                    "zoneIds": [f"{department}-"]
                }
            }
            
            response = self.fetch_annonces(filters)
            
            if not response:
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    print(f"      ❌ Trop d'erreurs département {department}")
                    break
                time.sleep(self.delay * 2)
                continue
            
            consecutive_errors = 0
            annonces = response.get('realEstateAds', [])
            total = response.get('total', 0)
            
            if not annonces or total == 0:
                break
            
            result = self.save_annonces(annonces)
            
            self.stats['total_scraped'] += len(annonces)
            self.stats['inserted'] += result['inserted']
            self.stats['updated'] += result['updated']
            self.stats['skipped'] += result['skipped']
            
            print(f"      📄 Page {page_num}: {len(annonces)} annonces "
                  f"(🆕 {result['inserted']}, 🔄 {result['updated']}) "
                  f"- {from_index + len(annonces)}/{total}", flush=True)
            
            from_index += len(annonces)
            
            if from_index >= total:
                break
            
            page_num += 1
            time.sleep(self.delay)
    
    def scrape_with_filters(self, filter_type: str, property_types: List[str]):
        """Scraper les annonces avec des filtres par département"""
        print(f"\n{'='*60}")
        print(f"🚀 Début du scraping: {filter_type.upper()} - {property_types}")
        print(f"   Stratégie: Scraping par département (96 départements)")
        print(f"{'='*60}\n")
        
        for property_type in property_types:
            print(f"\n📦 Scraping {property_type}...")
            
            for i, dept in enumerate(self.departments, 1):
                print(f"  📍 Département {dept} ({i}/{len(self.departments)})...")
                self.scrape_with_department_filter(filter_type, property_type, dept)
                
                # Stats intermédiaires tous les 10 départements
                if i % 10 == 0:
                    total_db = self.collection.count_documents({})
                    print(f"\n  📊 Progression: {i}/{len(self.departments)} départements")
                    print(f"     Total en DB: {total_db} annonces\n")
    
    def scrape_all(self):
        """Scraper toutes les annonces de vente"""
        start_time = time.time()
        
        print("\n" + "="*60)
        print("🏠 SCRAPER BIENICI - ANNONCES DE VENTE")
        print("🌍 Mode: SCRAPING COMPLET (tous départements)")
        print("="*60)
        
        self.scrape_with_filters(
            filter_type='buy',
            property_types=['flat', 'house']
        )
        
        duration = time.time() - start_time
        self.print_stats(duration)
    
    def print_stats(self, duration: float):
        """Afficher les statistiques finales"""
        print("\n" + "="*60)
        print("📊 STATISTIQUES FINALES")
        print("="*60)
        print(f"  Total scrapé:     {self.stats['total_scraped']}")
        print(f"  Nouvelles:        {self.stats['inserted']}")
        print(f"  Mises à jour:     {self.stats['updated']}")
        print(f"  Ignorées:         {self.stats['skipped']}")
        print(f"  Erreurs:          {self.stats['errors']}")
        print(f"  Durée:            {duration:.2f}s ({duration/60:.1f}min)")
        if duration > 0:
            print(f"  Vitesse:          {self.stats['total_scraped'] / duration:.2f} annonces/s")
        
        # Stats MongoDB
        total_db = self.collection.count_documents({})
        print(f"\n  Total en DB:      {total_db}")
        print("="*60 + "\n")
    
    def close(self):
        """Fermer la connexion MongoDB"""
        self.client.close()


def main():
    """Point d'entrée principal"""
    scraper = BieniciScraper()
    
    try:
        scraper.scrape_all()
    except KeyboardInterrupt:
        print("\n\n⚠️  Scraping interrompu")
    except Exception as e:
        print(f"\n\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
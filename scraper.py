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

class BieniciScraperLocations:
    def __init__(self):
        # Configuration MongoDB
        self.mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        self.db_name = os.getenv('MONGODB_DATABASE', 'bienici')
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.collection = self.db['locations']  # Collection séparée pour locations
        
        # Configuration Scraper
        self.api_url = os.getenv('BIENICI_API_URL', 'https://www.bienici.com/realEstateAds.json')
        self.delay = int(os.getenv('DELAY_BETWEEN_REQUESTS', 2))
        self.max_pages = int(os.getenv('MAX_PAGES', 100))
        self.items_per_page = int(os.getenv('ITEMS_PER_PAGE', 100))
        
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
        self.collection.create_index([('isFurnished', ASCENDING)])
        
        # Index composés pour locations
        self.collection.create_index([
            ('city', ASCENDING),
            ('propertyType', ASCENDING),
            ('price', ASCENDING)
        ])
        
        self.collection.create_index([
            ('price', ASCENDING),
            ('surfaceArea', ASCENDING)
        ])
        
        print("  ✅ Index de recherche créés\n")
    
    def fetch_annonces(self, filters: Dict) -> Optional[Dict]:
        """Récupérer les annonces depuis l'API Bienici"""
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'fr-FR,fr;q=0.9',
            'Referer': 'https://www.bienici.com',
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
    
    def extract_district_info(self, district_data):
        """Extraire les informations du district"""
        if not district_data:
            return {}
        
        if isinstance(district_data, dict):
            return {
                'district_name': district_data.get('name'),
                'district_libelle': district_data.get('libelle'),
                'district_id_polygone': district_data.get('id_polygone'),
                'district_insee_code': district_data.get('insee_code'),
            }
        return {}
    
    def extract_blur_info(self, blur_data):
        """Extraire les informations de géolocalisation floue"""
        if not blur_data:
            return {}
        
        result = {}
        if isinstance(blur_data, dict):
            position = blur_data.get('position', {})
            if position:
                result['blur_latitude'] = position.get('lat')
                result['blur_longitude'] = position.get('lon')
        
        return result
    
    def prepare_annonce(self, data: Dict) -> Dict:
        """Préparer et nettoyer les données d'une annonce de location"""
        
        # Extraire district info
        district_info = self.extract_district_info(data.get('district'))
        
        # Extraire blur info (position floue)
        blur_info = self.extract_blur_info(data.get('blurInfo'))
        
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
            'departmentCode': data.get('departmentCode'),
            'addressKnown': data.get('addressKnown'),
            'displayDistrictName': data.get('displayDistrictName'),
            
            # === PRIX LOCATION (champ "price" dans l'API pour les locations) ===
            'price': data.get('price'),  # Prix mensuel de location
            'agencyRentalFee': data.get('agencyRentalFee'),
            'safetyDeposit': data.get('safetyDeposit'),
            'charges': data.get('charges'),
            'chargesIncluded': data.get('chargesIncluded'),
            'chargesMethod': data.get('chargesMethod'),
            'rentExtra': data.get('rentExtra'),
            'inventoryOfFixturesFees': data.get('inventoryOfFixturesFees'),
            'pricePerSquareMeter': data.get('pricePerSquareMeter'),
            
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
            'terracesQuantity': data.get('terracesQuantity'),
            
            # === ÉTAT ===
            'newProperty': data.get('newProperty'),
            'yearOfConstruction': data.get('yearOfConstruction'),
            'condition': data.get('condition'),
            'isFurnished': data.get('isFurnished'),
            'isStudio': data.get('isStudio'),
            
            # === DATES ===
            'publicationDate': data.get('publicationDate'),
            'modificationDate': data.get('modificationDate'),
            'availableDate': data.get('availableDate'),
            
            # === TYPE ===
            'adType': data.get('adType'),
            'transactionType': data.get('transactionType'),
            'adTypeFR': data.get('adTypeFR'),
            'accountType': data.get('accountType'),
            'accountDisplayName': data.get('accountDisplayName'),
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
            'energyPerformanceDiagnosticDate': data.get('energyPerformanceDiagnosticDate'),
            'useJuly2021EnergyPerformanceDiagnostic': data.get('useJuly2021EnergyPerformanceDiagnostic'),
            'minEnergyConsumption': data.get('minEnergyConsumption'),
            'maxEnergyConsumption': data.get('maxEnergyConsumption'),
            'epdFinalEnergyConsumption': data.get('epdFinalEnergyConsumption'),
            
            # === PARKING ===
            'parkingPlacesQuantity': data.get('parkingPlacesQuantity'),
            'garagesQuantity': data.get('garagesQuantity'),
            
            # === MÉDIAS ===
            'photos': data.get('photos', []),
            'photosCount': len(data.get('photos', [])),
            'virtualTour': data.get('virtualTour'),
            'with3dModel': data.get('with3dModel'),
            
            # === AGENCE ===
            'agencyId': data.get('agencyId'),
            'agencyName': data.get('agencyName'),
            'agencyPhone': data.get('agencyPhone'),
            'agencyFeeUrl': data.get('agencyFeeUrl'),
            
            # === CONTACT ===
            'contactPhone': data.get('contactPhone'),
            'showContactForm': data.get('showContactForm'),
            'customerId': data.get('customerId'),
            'nothingBehindForm': data.get('nothingBehindForm'),
            'highlightMailContact': data.get('highlightMailContact'),
            
            # === STATUT ===
            'status': data.get('status'),
            'priceHasDecreased': data.get('priceHasDecreased'),
            'isBienIciExclusive': data.get('isBienIciExclusive'),
            'endOfPromotedAsExclusive': data.get('endOfPromotedAsExclusive'),
            
            # === DIVERS ===
            'hasGeorisquesMention': data.get('hasGeorisquesMention'),
            'opticalFiberStatus': data.get('opticalFiberStatus'),
            'displayInsuranceEstimation': data.get('displayInsuranceEstimation'),
            'descriptionTextLength': data.get('descriptionTextLength'),
            
            # === MÉTADONNÉES ===
            'scraped_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
        }
        
        # Ajouter les infos du district
        prepared.update(district_info)
        
        # Ajouter les infos de blur (position floue)
        prepared.update(blur_info)
        
        # Supprimer les None
        return {k: v for k, v in prepared.items() if v is not None}
    
    def save_annonces(self, annonces: List[Dict]) -> Dict:
        """Sauvegarder les annonces de location"""
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
                
                # Vérifier si price existe et est valide (pour les locations)
                if not prepared.get('price') or prepared.get('price') <= 0:
                    skipped += 1
                    continue
                
                # Vérifier que c'est bien une location
                if prepared.get('adType') != 'rent':
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
                skipped += 1
            except Exception as e:
                print(f"\n  ⚠️  Erreur annonce {annonce.get('id')}: {str(e)[:100]}")
                skipped += 1
        
        return {
            'inserted': inserted,
            'updated': updated,
            'skipped': skipped
        }
    
    def scrape_with_filters(self, property_types: List[str]):
        """Scraper les locations avec des filtres spécifiques"""
        print(f"\n{'='*60}")
        print(f"🏠 Scraping LOCATIONS - {property_types}")
        print(f"{'='*60}\n")
        
        for property_type in property_types:
            print(f"\n📦 Scraping {property_type} en location...")
            
            from_index = 0
            has_more = True
            page_num = 1
            
            while has_more and page_num <= self.max_pages:
                filters = {
                    "size": self.items_per_page,
                    "from": from_index,
                    "filterType": "rent",  # 🔥 LOCATION
                    "propertyType": [property_type],
                    "page": page_num,
                    "sortBy": "publicationDate",
                    "sortOrder": "desc",
                    "onTheMarket": [True]
                }
                
                print(f"  📄 Page {page_num} (index: {from_index})... ", end='', flush=True)
                
                response = self.fetch_annonces(filters)
                
                if not response:
                    print("❌ Pas de réponse")
                    break
                
                annonces = response.get('realEstateAds', [])
                total = response.get('total', 0)
                
                if not annonces:
                    print(f"✅ Aucune annonce")
                    break
                
                result = self.save_annonces(annonces)
                
                self.stats['total_scraped'] += len(annonces)
                self.stats['inserted'] += result['inserted']
                self.stats['updated'] += result['updated']
                self.stats['skipped'] += result['skipped']
                
                print(f"✅ {len(annonces)} annonces "
                      f"(🆕 {result['inserted']}, "
                      f"🔄 {result['updated']}, "
                      f"⏭️  {result['skipped']}) "
                      f"- {from_index + len(annonces)}/{total}")
                
                from_index += len(annonces)
                
                if from_index >= total:
                    print(f"  ✅ Terminé: {from_index}/{total}")
                    break
                
                page_num += 1
                time.sleep(self.delay)
    
    def scrape_all_rentals(self):
        """Scraper toutes les locations"""
        start_time = time.time()
        
        print("\n" + "="*60)
        print("🏠 SCRAPER BIENICI - ANNONCES DE LOCATION")
        print("="*60)
        
        # Scraper appartements et maisons en location
        self.scrape_with_filters(
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
        print(f"  Durée:            {duration:.2f}s")
        if duration > 0:
            print(f"  Vitesse:          {self.stats['total_scraped'] / duration:.2f} annonces/s")
        
        # Stats MongoDB
        total_db = self.collection.count_documents({})
        
        # Prix moyen
        avg_result = list(self.collection.aggregate([
            {'$group': {'_id': None, 'avg': {'$avg': '$price'}}}
        ]))
        
        # Par type de bien
        by_type = list(self.collection.aggregate([
            {'$group': {'_id': '$propertyType', 'count': {'$sum': 1}}}
        ]))
        
        # Par meublé/non meublé
        furnished = self.collection.count_documents({'isFurnished': True})
        unfurnished = self.collection.count_documents({'isFurnished': False})
        
        print(f"\n  📊 Base de données:")
        print(f"     Total locations:  {total_db}")
        if avg_result and avg_result[0].get('avg'):
            print(f"     Prix moyen:       {avg_result[0]['avg']:.2f}€/mois")
        
        if by_type:
            print(f"\n  📊 Par type:")
            for item in by_type:
                print(f"     {item['_id']}: {item['count']}")
        
        print(f"\n  📊 Meublé:")
        print(f"     Meublé:     {furnished}")
        print(f"     Non meublé: {unfurnished}")
        
        print("="*60 + "\n")
    
    def close(self):
        """Fermer la connexion MongoDB"""
        self.client.close()


def main():
    """Point d'entrée principal"""
    scraper = BieniciScraperLocations()
    
    try:
        scraper.scrape_all_rentals()
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
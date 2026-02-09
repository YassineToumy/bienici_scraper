#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================

SOURCE_COLLECTION = "locations"  # Collection source
CLEAN_COLLECTION = "locations_clean"  # Collection nettoyée pour ML
BATCH_SIZE = 500

# Prix min/max acceptables (en euros/mois)
MIN_PRICE = 200
MAX_PRICE = 10000

# Surface min/max acceptables (en m²)
MIN_SURFACE = 9
MAX_SURFACE = 500

# Nombre de pièces max acceptable
MAX_ROOMS = 20

# ============================================================
# CHAMPS POUR LE MODÈLE ML
# ============================================================

# Features pour le modèle de prédiction de prix
FIELDS_TO_KEEP = [
    # === IDENTIFICATION ===
    "id",
    "reference",
    "title",
    
    # === LOCALISATION (TRÈS IMPORTANT pour prix) ===
    "city",
    "postalCode",
    "departmentCode",
    "district_name",
    "district_libelle",
    "blur_latitude",  # Position floue
    "blur_longitude",
    
    # === TARGET & PRIX ===
    "price",  # TARGET du modèle
    "pricePerSquareMeter",
    "charges",
    "agencyRentalFee",
    "safetyDeposit",
    
    # === CARACTÉRISTIQUES PRINCIPALES ===
    "propertyType",  # flat/house
    "surfaceArea",  # Surface habitable
    "roomsQuantity",
    "bedroomsQuantity",
    "bathroomsQuantity",
    "showerRoomsQuantity",
    "floor",
    "floorQuantity",
    "terracesQuantity",
    
    # === ÉTAT & ÂGE ===
    "yearOfConstruction",
    "newProperty",
    "isFurnished",  # TRÈS IMPORTANT pour location
    
    # === ÉQUIPEMENTS EXTÉRIEURS ===
    "hasCellar",
    "hasBalcony",
    "hasTerrace",
    "hasGarden",
    "hasPool",
    
    # === ÉQUIPEMENTS INTÉRIEURS ===
    "hasElevator",
    "hasIntercom",
    "hasAirConditioning",
    "hasFireplace",
    "hasSeparateToilet",
    
    # === DPE (Diagnostic Performance Énergétique) ===
    "energyClassification",  # A, B, C, D, E, F, G
    "energyValue",
    "greenhouseGazClassification",
    "greenhouseGazValue",
    "heating",
    
    # === PARKING ===
    "parkingPlacesQuantity",
    "garagesQuantity",
    
    # === DATES ===
    "publicationDate",
    "availableDate",
    
    # === MÉTADONNÉES ===
    "scraped_at",
    "created_at",
]

# Projection MongoDB
PROJECTION = {"_id": 0}
for field in FIELDS_TO_KEEP:
    PROJECTION[field] = 1


# ============================================================
# FONCTIONS DE NETTOYAGE
# ============================================================

def is_valid_location(doc):
    """
    Valider qu'un document de location est exploitable pour le ML.
    
    Critères de validation :
    - Prix valide (entre MIN_PRICE et MAX_PRICE)
    - Surface valide (entre MIN_SURFACE et MAX_SURFACE)
    - Type de bien connu (flat ou house)
    - Nombre de pièces raisonnable
    """
    
    # 1. Prix valide
    price = doc.get("price")
    if not price or price < MIN_PRICE or price > MAX_PRICE:
        return False, "prix_invalide"
    
    # 2. Surface valide
    surface = doc.get("surfaceArea")
    if not surface or surface < MIN_SURFACE or surface > MAX_SURFACE:
        return False, "surface_invalide"
    
    # 3. Type de bien valide
    property_type = doc.get("propertyType")
    if property_type not in ["flat", "house"]:
        return False, "type_invalide"
    
    # 4. Localisation minimale
    if not doc.get("city") or not doc.get("postalCode"):
        return False, "localisation_manquante"
    
    # 5. Nombre de pièces raisonnable (si renseigné)
    rooms = doc.get("roomsQuantity")
    if rooms and rooms > MAX_ROOMS:
        return False, "pieces_aberrant"
    
    # 6. Prix au m² cohérent (si calculable)
    if surface and price:
        price_per_m2 = price / surface
        if price_per_m2 < 3 or price_per_m2 > 100:  # Entre 3€ et 100€/m²
            return False, "prix_m2_aberrant"
    
    return True, None


def clean_document(doc):
    """
    Nettoyer et enrichir un document pour le ML.
    
    Transformations :
    - Convertir les booléens None en False
    - Calculer l'âge du bien
    - Calculer le prix au m² si manquant
    - Normaliser les champs texte
    """
    
    cleaned = doc.copy()
    
    # === BOOLÉENS : None → False ===
    boolean_fields = [
        "isFurnished", "newProperty", "hasCellar", "hasBalcony", 
        "hasTerrace", "hasGarden", "hasPool", "hasElevator", 
        "hasIntercom", "hasAirConditioning", "hasFireplace",
        "hasSeparateToilet"
    ]
    
    for field in boolean_fields:
        if field in cleaned and cleaned[field] is None:
            cleaned[field] = False
    
    # === FEATURE ENGINEERING ===
    
    # 1. Âge du bien
    year_construction = cleaned.get("yearOfConstruction")
    if year_construction and year_construction > 1800:
        cleaned["age_of_property"] = 2026 - year_construction
    else:
        cleaned["age_of_property"] = None
    
    # 2. Prix au m² (recalculer si manquant)
    if cleaned.get("surfaceArea") and cleaned.get("price"):
        if not cleaned.get("pricePerSquareMeter"):
            cleaned["pricePerSquareMeter"] = cleaned["price"] / cleaned["surfaceArea"]
    
    # 3. Ratio pièces/surface
    if cleaned.get("roomsQuantity") and cleaned.get("surfaceArea"):
        cleaned["room_surface_ratio"] = cleaned["surfaceArea"] / cleaned["roomsQuantity"]
    else:
        cleaned["room_surface_ratio"] = None
    
    # 4. Score d'équipements
    equipment_score = 0
    if cleaned.get("hasElevator"): equipment_score += 1
    if cleaned.get("hasParking") or cleaned.get("parkingPlacesQuantity", 0) > 0: equipment_score += 1
    if cleaned.get("hasBalcony") or cleaned.get("hasTerrace"): equipment_score += 1
    if cleaned.get("hasAirConditioning"): equipment_score += 1
    if cleaned.get("isFurnished"): equipment_score += 1
    cleaned["equipment_score"] = equipment_score
    
    # 5. Normaliser le type de chauffage (si renseigné)
    heating = cleaned.get("heating")
    if heating:
        heating_lower = heating.lower()
        if "individuel" in heating_lower:
            cleaned["heating_type_normalized"] = "individual"
        elif "collectif" in heating_lower:
            cleaned["heating_type_normalized"] = "collective"
        else:
            cleaned["heating_type_normalized"] = "other"
    else:
        cleaned["heating_type_normalized"] = None
    
    # 6. Classe énergétique en numérique
    energy_class = cleaned.get("energyClassification")
    energy_map = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7}
    cleaned["energy_class_numeric"] = energy_map.get(energy_class, None)
    
    # === MÉTADONNÉES ===
    cleaned["cleaned_at"] = datetime.utcnow()
    
    return cleaned


# ============================================================
# FONCTIONS PRINCIPALES
# ============================================================

def connect_db():
    """Connexion à MongoDB."""
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGODB_DATABASE", "bienici")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    return client, db


def setup_clean_collection(db):
    """Préparer la collection propre."""
    clean = db[CLEAN_COLLECTION]
    clean.drop()
    print(f"🗑️  Collection '{CLEAN_COLLECTION}' réinitialisée")
    
    # Index
    clean.create_index([("id", ASCENDING)], unique=True, name="id_unique")
    clean.create_index([("city", ASCENDING)])
    clean.create_index([("postalCode", ASCENDING)])
    clean.create_index([("propertyType", ASCENDING)])
    clean.create_index([("price", ASCENDING)])
    clean.create_index([("surfaceArea", ASCENDING)])
    clean.create_index([("isFurnished", ASCENDING)])
    print("✅ Index créés\n")
    
    return clean


def fetch_clean_store(source, clean):
    """Pipeline principal : Fetch → Clean → Store"""
    
    total = source.count_documents({})
    print(f"📊 Documents dans '{SOURCE_COLLECTION}': {total}\n")
    
    if total == 0:
        print("⚠️  Aucun document à traiter.")
        return
    
    # Statistiques
    stats = {
        "inserted": 0,
        "prix_invalide": 0,
        "surface_invalide": 0,
        "type_invalide": 0,
        "localisation_manquante": 0,
        "pieces_aberrant": 0,
        "prix_m2_aberrant": 0,
        "duplicates": 0
    }
    
    batch = []
    cursor = source.find({}, PROJECTION, batch_size=BATCH_SIZE)
    
    for doc in cursor:
        # VALIDATION
        is_valid, reason = is_valid_location(doc)
        
        if not is_valid:
            stats[reason] = stats.get(reason, 0) + 1
            continue
        
        # NETTOYAGE & ENRICHISSEMENT
        cleaned_doc = clean_document(doc)
        batch.append(cleaned_doc)
        
        # INSERTION PAR BATCH
        if len(batch) >= BATCH_SIZE:
            result = insert_batch(clean, batch)
            stats["inserted"] += result["inserted"]
            stats["duplicates"] += result["duplicates"]
            batch = []
            
            processed = sum(stats.values())
            print(f"   ⏳ {processed}/{total} traités "
                  f"(✅ {stats['inserted']} valides)", 
                  end="\r", flush=True)
    
    # Dernier batch
    if batch:
        result = insert_batch(clean, batch)
        stats["inserted"] += result["inserted"]
        stats["duplicates"] += result["duplicates"]
    
    print_stats(stats, total)


def insert_batch(collection, batch):
    """Insérer un batch."""
    inserted = 0
    duplicates = 0
    try:
        result = collection.insert_many(batch, ordered=False)
        inserted = len(result.inserted_ids)
    except BulkWriteError as e:
        inserted = e.details.get("nInserted", 0)
        duplicates = len(batch) - inserted
    return {"inserted": inserted, "duplicates": duplicates}


def print_stats(stats, total):
    """Afficher les statistiques de nettoyage."""
    rejected = total - stats["inserted"] - stats["duplicates"]
    
    print(f"\n\n{'='*60}")
    print(f"📊 RÉSULTATS DU NETTOYAGE")
    print(f"{'='*60}")
    print(f"   📥 Total traités:              {total}")
    print(f"   ✅ Valides (insérés):          {stats['inserted']} ({stats['inserted']/total*100:.1f}%)")
    print(f"   ❌ Rejetés:                    {rejected} ({rejected/total*100:.1f}%)")
    
    if rejected > 0:
        print(f"\n   📋 Détail des rejets:")
        print(f"      💰 Prix invalide:           {stats.get('prix_invalide', 0)}")
        print(f"      📏 Surface invalide:        {stats.get('surface_invalide', 0)}")
        print(f"      🏠 Type invalide:           {stats.get('type_invalide', 0)}")
        print(f"      📍 Localisation manquante:  {stats.get('localisation_manquante', 0)}")
        print(f"      🔢 Nb pièces aberrant:      {stats.get('pieces_aberrant', 0)}")
        print(f"      💵 Prix/m² aberrant:        {stats.get('prix_m2_aberrant', 0)}")
    
    if stats["duplicates"] > 0:
        print(f"\n   🔁 Doublons ignorés:           {stats['duplicates']}")
    
    print(f"{'='*60}")


def analyze_clean_data(clean):
    """Analyser la qualité des données nettoyées."""
    print(f"\n{'='*60}")
    print(f"🔍 ANALYSE DES DONNÉES NETTOYÉES")
    print(f"{'='*60}")
    
    total = clean.count_documents({})
    print(f"   📊 Total documents:            {total}")
    
    # Prix
    price_stats = list(clean.aggregate([
        {"$group": {
            "_id": None,
            "avg": {"$avg": "$price"},
            "min": {"$min": "$price"},
            "max": {"$max": "$price"}
        }}
    ]))
    
    if price_stats:
        ps = price_stats[0]
        print(f"\n   💰 Prix de location:")
        print(f"      Moyen:    {ps['avg']:.2f}€/mois")
        print(f"      Min:      {ps['min']:.2f}€/mois")
        print(f"      Max:      {ps['max']:.2f}€/mois")
    
    # Surface
    surface_stats = list(clean.aggregate([
        {"$group": {
            "_id": None,
            "avg": {"$avg": "$surfaceArea"},
            "min": {"$min": "$surfaceArea"},
            "max": {"$max": "$surfaceArea"}
        }}
    ]))
    
    if surface_stats:
        ss = surface_stats[0]
        print(f"\n   📏 Surface:")
        print(f"      Moyenne:  {ss['avg']:.2f}m²")
        print(f"      Min:      {ss['min']:.2f}m²")
        print(f"      Max:      {ss['max']:.2f}m²")
    
    # Par type
    by_type = list(clean.aggregate([
        {"$group": {"_id": "$propertyType", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]))
    
    if by_type:
        print(f"\n   🏠 Par type de bien:")
        for item in by_type:
            print(f"      {item['_id']}: {item['count']} ({item['count']/total*100:.1f}%)")
    
    # Meublé/Non meublé
    furnished = clean.count_documents({"isFurnished": True})
    unfurnished = clean.count_documents({"isFurnished": False})
    
    print(f"\n   🛋️  Ameublement:")
    print(f"      Meublé:     {furnished} ({furnished/total*100:.1f}%)")
    print(f"      Non meublé: {unfurnished} ({unfurnished/total*100:.1f}%)")
    
    # Champs manquants critiques
    missing = {
        "roomsQuantity": clean.count_documents({"roomsQuantity": None}),
        "bedroomsQuantity": clean.count_documents({"bedroomsQuantity": None}),
        "floor": clean.count_documents({"floor": None}),
        "energyClassification": clean.count_documents({"energyClassification": None}),
    }
    
    print(f"\n   ⚠️  Valeurs manquantes:")
    for field, count in missing.items():
        if count > 0:
            print(f"      {field}: {count} ({count/total*100:.1f}%)")
    
    # Top 10 villes
    top_cities = list(clean.aggregate([
        {"$group": {"_id": "$city", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]))
    
    if top_cities:
        print(f"\n   🏙️  Top 10 villes:")
        for i, city in enumerate(top_cities, 1):
            print(f"      {i}. {city['_id']}: {city['count']}")
    
    print(f"{'='*60}\n")


# ============================================================
# MAIN
# ============================================================

def main():
    print("\n" + "="*60)
    print("🧹 NETTOYAGE DES DONNÉES POUR MODÈLE ML")
    print("   Location Price Prediction")
    print("="*60 + "\n")
    
    client, db = connect_db()
    source = db[SOURCE_COLLECTION]
    clean = setup_clean_collection(db)
    
    fetch_clean_store(source, clean)
    analyze_clean_data(clean)
    
    print("✅ Données prêtes pour l'entraînement du modèle !")
    print(f"   Collection: {CLEAN_COLLECTION}")
    print(f"   Prochaine étape: Extraction vers CSV/DataFrame\n")
    
    client.close()


if __name__ == "__main__":
    main()
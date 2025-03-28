import os
import pymongo
import spacy
import re
from dotenv import load_dotenv
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer

# --- CHARGEMENT DES VARIABLES D'ENVIRONNEMENT ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# --- CONNEXION À MONGODB ---
client = pymongo.MongoClient(MONGO_URI)
db = client["job_finder"]
collection = db["offers"]

# --- CHARGER LE MODÈLE NLP (Français) ---
nlp = spacy.load("fr_core_news_md")

# --- STOPWORDS PERSONNALISÉS (optionnel) ---
CUSTOM_STOPWORDS = {"emploi", "offre", "recherche", "travail", "poste"}

# --- FONCTION 1 : NETTOYAGE DE LA DESCRIPTION ---
def clean_description(text):
    if not text:
        return ""

    text = text.lower()  # Mettre en minuscules
    text = re.sub(r"\s+", " ", text)  # Supprimer les espaces en trop
    text = re.sub(r"https?://\S+|www\.\S+", "", text)  # Supprimer les liens
    return text.strip()

# --- FONCTION 2 : EXTRACTION DE MOTS-CLÉS ---
def extract_keywords(text):
    """ Extrait les mots-clés en utilisant NLP (lemmatisation) + TF-IDF """
    if not text:
        return []

    doc = nlp(text)
    keywords = [token.lemma_ for token in doc if token.is_alpha and not token.is_stop and token.lemma_ not in CUSTOM_STOPWORDS]
    
    return list(set(keywords))  # Supprime les doublons

# --- FONCTION 3 : STANDARDISATION DE L’EXPÉRIENCE ---
def extract_experience(text):
    """ Convertit une expérience textuelle en format numérique (min/max années) """
    if not text:
        return {"min": None, "max": None}

    experience = re.findall(r"(\d+)\s*ans?", text)
    experience = list(map(int, experience))  # Convertir en nombres entiers

    if len(experience) == 1:
        return {"min": experience[0], "max": experience[0]}
    elif len(experience) > 1:
        return {"min": min(experience), "max": max(experience)}
    
    return {"min": None, "max": None}

# --- FONCTION PRINCIPALE : METTRE À JOUR MONGODB ---
def process_offers():
    """ Applique le NLP sur toutes les offres et met à jour MongoDB """
    offers = collection.find({})  # Récupérer toutes les offres

    for offer in offers:
        # Nettoyage et NLP
        offer["description"] = clean_description(offer.get("description", ""))
        offer["keywords"] = extract_keywords(f"{offer.get('title', '')} {offer.get('description', '')} {offer.get('skills', '')}")
        offer["experience"] = extract_experience(offer.get("experience", ""))

        # Mise à jour dans MongoDB
        collection.update_one({"_id": offer["_id"]}, {"$set": offer})

    print("✅ NLP Processing terminé : données mises à jour !")

# --- EXÉCUTER LE TRAITEMENT ---
if __name__ == "__main__":
    process_offers()

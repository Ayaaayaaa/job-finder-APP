import os
from dotenv import load_dotenv
import pymongo

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["job_finder"]
    collection = db["offers"]
    print("✅ Connexion réussie à MongoDB Atlas !")
except Exception as e:
    print(f"❌ Erreur de connexion : {e}")

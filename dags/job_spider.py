import os
import scrapy
import pymongo
import logging
import random
import requests
from scrapy.crawler import CrawlerProcess
from dotenv import load_dotenv
from datetime import datetime
from fake_useragent import UserAgent
from urllib.parse import urljoin

# Configuration des logs
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler('job_spider.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Connexion à MongoDB
try:
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client["job_finder"]
    collection = db["offers"]
    logger.info("✅ Connexion réussie à MongoDB")
except Exception as e:
    logger.error(f"❌ Erreur de connexion à MongoDB: {e}")
    exit(1)

class JobSpider(scrapy.Spider):
    name = "emploi_ma_spider"
    
    def start_requests(self):
        # URLs de base avec différentes configurations
        base_urls = [
            "https://www.emploi.ma/recherche-jobs-maroc",
            "https://www.emploi.ma/recherche-jobs-maroc?utm_source=site&utm_medium=link&utm_campaign=search_split&utm_term=all_jobs"
        ]
        
        # Configuration de la pagination
        max_pages = 5
        
        for base_url in base_urls:
            for page in range(max_pages):
                # Gestion de la pagination
                url = f"{base_url}&page={page}" if page > 0 else base_url
                
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_jobs,
                    meta={'page': page, 'base_url': base_url}
                )
    
    def parse_jobs(self, response):
        page = response.meta.get('page', 0)
        base_url = response.meta.get('base_url', '')
        
        # Stratégies de sélection des jobs
        job_selectors = [
            "div.page-search-jobs-content .card.card-job.featured",
            "div.page-search-jobs-content .card.card-job",
            ".card.card-job.featured",
            ".card.card-job"
        ]
        
        jobs_found = False
        jobs_to_insert = []
        
        for selector in job_selectors:
            jobs = response.css(selector)
            
            if jobs:
                logger.info(f"Sélecteur '{selector}' a trouvé {len(jobs)} jobs")
                jobs_found = True
                
                for job in jobs:
                    job_data = self._extract_job_data(job, response)
                    
                    if job_data:
                        # Vérification des doublons avant insertion
                        if not collection.find_one({"link": job_data["link"]}):
                            jobs_to_insert.append(job_data)
                
                break  # Stop après le premier sélecteur qui fonctionne
        
        # Insertion en masse dans MongoDB
        if jobs_to_insert:
            try:
                collection.insert_many(jobs_to_insert)
                logger.info(f"✅ {len(jobs_to_insert)} nouvelles offres insérées depuis la page {page}")
            except Exception as e:
                logger.error(f"❌ Erreur d'insertion MongoDB: {e}")
        
        if not jobs_found:
            logger.warning(f"Aucun job trouvé sur la page {page} avec l'URL {response.url}")
            # Log du HTML pour débogage
            with open(f'debug_page_{page}.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
    
    def _extract_job_data(self, job, response):
        """Extraction structurée et robuste des données"""
        try:
            # URLs relatives complétées
            link = job.css("h3 a::attr(href)").get() or job.attrib.get("data-href", "")
            full_link = urljoin('https://www.emploi.ma', link)
            
            return {
                "title": job.css("h3 a::text").get(default="").strip(),
                "company": job.css("a.card-job-company::text, .card-job-company::text").get(default="").strip(),
                "location": self._extract_list_detail(job, "Région de"),
                "contract": self._extract_list_detail(job, "Contrat proposé"),
                "experience": self._extract_list_detail(job, "Niveau d'expérience"),
                "education": self._extract_list_detail(job, "Niveau d´études requis"),
                "description": job.css("div.card-job-description p::text").get(default="").strip(),
                "skills": self._extract_list_detail(job, "Compétences clés"),
                "link": full_link,
                "date_posted": job.css("time::text").get(default="").strip(),
                "date_scraped": datetime.utcnow().isoformat(),
                "source": "emploi.ma"
            }
        except Exception as e:
            logger.error(f"Erreur d'extraction de l'offre : {e}")
            return None
    
    def _extract_list_detail(self, job, key):
        """Extraction améliorée des détails de liste"""
        list_items = job.css("ul li")
        for item in list_items:
            item_text = item.get()
            if key in item_text:
                return item.css("strong::text").get(default="").strip()
        return ""

# Configuration et lancement
def run_scraper():
    logger.info("🚀 Démarrage du scraper...")
    process = CrawlerProcess(settings={
        'LOG_LEVEL': 'INFO',
        'FEEDS': {
            'results.json': {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
            }
        },
        'USER_AGENT': UserAgent().random,
    })
    process.crawl(JobSpider)
    process.start()
    logger.info("✅ Scraping terminé.")

if __name__ == "__main__":
    run_scraper()
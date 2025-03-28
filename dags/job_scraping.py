# Importations des bibliothèques nécessaires
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

# Configuration par défaut du DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 21),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Création de l'instance DAG
dag = DAG(
    "job_scraping_pipeline",
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

# ✅ Utilisation de chemins absolus pour Airflow (Docker)
AIRFLOW_HOME = "/usr/local/airflow"
SCRAPER_SCRIPT = os.path.join(AIRFLOW_HOME, "dags/job_spider.py")
NLP_SCRIPT = os.path.join(AIRFLOW_HOME, "dags/nlp_processing.py")

# Fonction pour exécuter le script de scraping
def run_scraper():
    try:
        result = subprocess.run(
            ["/usr/bin/env", "python3", SCRAPER_SCRIPT],  # ✅ Exécution avec python3
            capture_output=True,
            text=True,
            check=True
        )
        print("✅ Scraping terminé avec succès:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors du scraping: {e}")
        print(f"Sortie d'erreur: {e.stderr}")
        raise

# Fonction pour exécuter le traitement NLP
def run_nlp_processing():
    try:
        result = subprocess.run(
            ["/usr/bin/env", "python3", NLP_SCRIPT],
            capture_output=True,
            text=True,
            check=True
        )
        print("✅ Traitement NLP terminé avec succès:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors du traitement NLP: {e}")
        print(f"Sortie d'erreur: {e.stderr}")
        raise

# Définition des tâches
scraping_task = PythonOperator(
    task_id="run_scraper",
    python_callable=run_scraper,
    dag=dag,
)

nlp_processing_task = PythonOperator(
    task_id="run_nlp_processing",
    python_callable=run_nlp_processing,
    dag=dag,
)

# Orchestration des tâches
scraping_task >> nlp_processing_task

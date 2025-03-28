FROM quay.io/astronomer/astro-runtime:12.7.1
# Installer les dépendances supplémentaires
RUN pip install pymongo scrapy spacy scikit-learn requests fake-useragent
RUN python -m spacy download fr_core_news_md
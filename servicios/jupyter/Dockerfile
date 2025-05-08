FROM jupyter/base-notebook:python-3.11

##########################
#### Instalar Java #######
##########################
USER root
RUN apt-get update && apt-get install -y default-jdk && apt-get clean
USER jovyan

# Instalar librerías Python
RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    boto3 \
    python-dotenv \
    ipykernel \
    pyspark \
    findspark






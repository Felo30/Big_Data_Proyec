FROM jupyter/base-notebook:python-3.11

##########################
#### Instalar Java #######
##########################
USER root
RUN apt-get update && apt-get install -y default-jdk && apt-get clean

# Descargar el driver JDBC de PostgreSQL
RUN curl -L -o /usr/local/spark/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

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
    findspark \
    pyarrow

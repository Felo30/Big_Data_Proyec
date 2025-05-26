##############################################
# Imagen base oficial de Airflow con Python 3.11
##############################################
FROM apache/airflow:2.9.1-python3.11

#######################################
# [1] Instalar dependencias del sistema
#######################################
USER root
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl && \
    apt-get clean

##############################################
# [2] Descargar Spark Standalone Client (para usar spark-submit)
##############################################
RUN curl -L -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /opt/ && \
    ln -s /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin

##########################################
# [3] JARs para Spark (PostgreSQL y S3/MinIO)
##########################################
RUN mkdir -p /usr/local/spark/jars && \
    curl -L -o /usr/local/spark/jars/postgresql-42.7.3.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    curl -L -o /usr/local/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /usr/local/spark/jars/aws-java-sdk-bundle-1.11.901.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar

#############################################
# [4] Instalar librerías de Python necesarias
#############################################
USER airflow

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
    ##############################
# Base: Jupyter + Python 3.11
##############################
FROM jupyter/base-notebook:python-3.11

#######################################
# [1] INSTALAR JAVA (requerido por Spark)
#######################################
USER root
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl && \
    apt-get clean

###############################################
# [2] CREAR CARPETA PARA JARS DE SPARK (si falta)
###############################################
RUN mkdir -p /usr/local/spark/jars

#######################################################
# [3] DESCARGAR DRIVERS Y DEPENDENCIAS PARA SPARK
#     - PostgreSQL JDBC
#     - MinIO/S3A (hadoop-aws + aws-sdk)
#######################################################
# PostgreSQL JDBC driver
RUN curl -L -o /usr/local/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Hadoop-AWS & AWS SDK bundle (para usar s3a:// en Spark)
RUN curl -L -o /usr/local/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /usr/local/spark/jars/aws-java-sdk-bundle-1.11.901.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar

####################################
# [4] VOLVER AL USUARIO JOVYAN
####################################
USER jovyan

#############################################
# [5] INSTALAR LIBRERÍAS PYTHON NECESARIAS
#############################################
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
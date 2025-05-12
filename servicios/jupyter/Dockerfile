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

import boto3
import psycopg2
from kafka import KafkaConsumer
from sqlalchemy import create_engine
import json
from dynaconf import settings

# attraverso l'uso della libreria dynaconf leggiamo le configurazioni di connessione dal file settings.toml

# connessione a s3 tramite il modulo boto3
def s3_connection():
    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id=settings.S3.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.S3.AWS_SECRET_ACCESS_KEY,
        endpoint_url=settings.S3.ENDPOINT_URL
    )
    return s3_client


# connessione a postgres tramite il modulo psycopg2
def pg_psycopg2_connection():
    conn = psycopg2.connect(
        host=settings.PG_PSYCOPG.HOST,
        port=settings.PG_PSYCOPG.PORT,
        database=settings.PG_PSYCOPG.DB,
        user=settings.PG_PSYCOPG.USER,
        password=settings.PG_PSYCOPG.PWD)
    return conn


# connessione a postgres tramite il modulo sqlalchemy
def pg_sqlalchemy_connection():
    conn = create_engine('postgresql://' +
                         settings.PG_SQLALCHEMY.USER + ':' +
                         settings.PG_SQLALCHEMY.PWD + '@' +
                         settings.PG_SQLALCHEMY.HOST + ':' +
                         settings.PG_SQLALCHEMY.PORT + '/' +
                         settings.PG_SQLALCHEMY.DB)
    return conn


# connessione a kafka tramite il modulo kafka-python
def kafka_connection():
    conn = KafkaConsumer(bootstrap_servers=[settings.KAFKA.HOST + ':' + settings.KAFKA.PORT], auto_offset_reset='earliest',
                         enable_auto_commit=True, auto_commit_interval_ms=1000, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    return conn


# connessione simulata per la lettura/scrittura di un file localmente
def local_file_connection():
    path = "/tmp/"
    return path


# dizionario delle connessioni, ogni chiave rappresenta un tipo di connessione, ogni valore è la funzione che crea la corrispondente connessione
conn_options = {
    "s3": s3_connection,
    "pg-psycopg2": pg_psycopg2_connection,
    "pg-sqlalchemy": pg_sqlalchemy_connection,
    "kafka": kafka_connection,
    "local_file": local_file_connection
}
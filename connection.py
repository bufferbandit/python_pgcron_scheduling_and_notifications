
import psycopg
import secrets


# Find a way to parameterize all of the queries


DBNAME = "postgres"
HOST = "192.168.114.37"
PORT = 5432
SCHEMA = "cron"



conn = psycopg.connect(
    dbname=DBNAME,
    user=secrets.USERNAME,
    password=secrets.PASSWORD,
    host=HOST,
    port=PORT,
    # schema="public",
    autocommit=True,
    options="-c search_path=" + SCHEMA,
)

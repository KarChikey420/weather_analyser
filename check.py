import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("REDSHIFT_HOST").split(":")[0],
    dbname=os.getenv("REDSHIFT_DB"),
    user=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD"),
    port=int(os.getenv("REDSHIFT_PORT",5439))
)

cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM your_table_name;")
count = cur.fetchone()[0]
print(f"Total rows in table: {count}")

cur.close()
conn.close()

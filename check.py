import psycopg2

try:
    conn = psycopg2.connect(
        host="weather-etl.220402651988.ap-south-1.redshift-serverless.amazonaws.com",
        port=5439,
        dbname="dev",
        user="admin",
        password="Morning0000"
    )
    print("✅ Connection successful!")
except Exception as e:
    print("❌ Connection failed:", e)

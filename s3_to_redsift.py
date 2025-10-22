import psycopg2

def load_data_to_redshift(table_name: str,
                          s3_bucket: str,
                          s3_key: str,
                          iam_role_arn: str,
                          host: str,
                          dbname: str,
                          user: str,
                          password: str,
                          port: int = 5439,
                          region: str = "ap-south-1"):
    
    s3_path = f"s3://{s3_bucket}/{s3_key}"
    
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = True
        cursor = conn.cursor()
        print("Connected to Redshift")

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            City_Name VARCHAR(50),
            Country_Name VARCHAR(50),
            Humidity_Percent FLOAT,
            Weather_Type VARCHAR(50),
            WindSpeed FLOAT,
            Date DATE,
            Time VARCHAR(20),
            Hour_of_Day INT,
            Day_of_Week VARCHAR(20),
            month INT,
            Temperature FLOAT,
            Hot_Day BOOLEAN,
            Rainy_Day BOOLEAN,
            Night_Time BOOLEAN
        );
        """
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' ready")

        copy_query = f"""
        COPY {table_name}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role_arn}'
        FORMAT AS CSV
        IGNOREHEADER 1
        REGION '{region}';
        """
        cursor.execute(copy_query)
        print(f"Data loaded successfully into '{table_name}'")

    except Exception as e:
        print(f"Error loading data to Redshift: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Redshift connection closed")

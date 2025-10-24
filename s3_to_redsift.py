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
    
    conn = None
    cursor = None
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

        drop_query = f"DROP TABLE IF EXISTS {table_name};"
        cursor.execute(drop_query)
        print(f"Table '{table_name}' dropped (if existed)")

        create_table_query = f"""
        CREATE TABLE {table_name} (
            City_Name VARCHAR(50),
            Country_Name VARCHAR(50),
            Humidity_Percent FLOAT,
            Weather_Type VARCHAR(50),
            WindSpeed FLOAT,
            Date DATE,
            Time VARCHAR(20),
            Hour_of_Day INT,
            Day_of_Week VARCHAR(20),
            month VARCHAR(20),
            Temperature FLOAT,
            Hot_Day BOOLEAN,
            Rainy_Day BOOLEAN,
            Night_Time BOOLEAN
            );
        """
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created")

        copy_query = f"""
        COPY {table_name}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role_arn}'
        FORMAT AS CSV
        IGNOREHEADER 1
        REGION '{region}'
        TIMEFORMAT 'auto'
        EMPTYASNULL
        BLANKSASNULL;
        """
        cursor.execute(copy_query)
        print(f"Data loaded successfully into '{table_name}'")

    except Exception as e:
        print(f"Error loading data to Redshift: {e}")
        try:
            if cursor:
                cursor.execute("SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 10;")
                errors = cursor.fetchall()
                if errors:
                    print("Recent Redshift load errors:")
                    for err in errors:
                        print(err)
        except Exception as inner_e:
            print(f"Could not fetch load errors: {inner_e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Redshift connection closed")

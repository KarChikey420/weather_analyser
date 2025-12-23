import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
from io import StringIO

load_dotenv()

def upload_df_to_s3(df, bucket_name, s3_key=None):
    current_date = datetime.now().strftime("%Y-%m-%d")

    if not s3_key:
        s3_key = f"transformed/{current_date}_weather_data.csv"

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        print(f"Uploaded to s3://{bucket_name}/{s3_key}")
        return s3_key

    except Exception as e:
        raise Exception(f"S3 upload failed: {e}")

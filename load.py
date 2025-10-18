import boto3
from datetime import datetime

def upload_to_s3(file_name, bucket_name):
    """
    Upload a CSV file to Amazon S3 with date-based naming.
    """
    # Initialize S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id='YOUR_AWS_ACCESS_KEY',
        aws_secret_access_key='YOUR_AWS_SECRET_KEY',
        region_name='ap-south-1'
    )

    # Create a date-based filename
    current_date = datetime.now().strftime("%Y-%m-%d")
    object_name = f"transformed/{current_date}_weather_data.csv"

    # Upload the file
    s3.upload_file(file_name, bucket_name, object_name)
    print(f"✅ Uploaded {file_name} → s3://{bucket_name}/{object_name}")

# Example usage
upload_to_s3("data/transformed_weather.csv", "weather-data-kartikey")


{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "s3-object-lambda:*"
            ],
            "Resource": "*"
        }
    ]
}
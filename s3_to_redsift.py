import psycopg2
import os

def load_data_to_redshift(table_name=str,
                          s3_bucket=str,
                          s3_key=str,
                          iam_role_arn=str,
                          host=str,
                          dbname=str,
                          user=str,
                          password=str,
                          port=int,
                          region=str):
    
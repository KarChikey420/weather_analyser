# 🌦️ Weather Analyser

A Python-based **ETL (Extract, Transform, Load)** project that automatically collects weather data from an API, processes it, and stores it into AWS S3 and Redshift for further analysis.


## 💡 Motivation
This project was created to build a reliable pipeline that collects real-time weather data and loads it into a data warehouse for analytics.  
It automates the entire process from **data extraction to loading**, helping analysts focus on insights instead of manual data collection.

---

## 🚀 Features
- 🌍 Fetches weather data using external APIs  
- 🧹 Cleans and transforms raw weather data  
- ☁️ Uploads transformed data to AWS S3  
- 🗄️ Loads the data from S3 to Amazon Redshift  
- 🧩 Modular and easily extendable for more sources or outputs  
- 💻 Includes scripts and notebooks for easy testing and customization  

---

## 🧭 Architecture & Workflow
**ETL Flow:**


1. **Extract**: Pulls data from a weather API  
2. **Transform**: Cleans, formats, and enriches the data  
3. **Load**: Uploads the final dataset to S3/Redshift  
4. **App Layer**: Optional web or script-based trigger via `app.py` or `main.py`

---

## ⚙️ Getting Started

### 🧾 Prerequisites
- Python 3.7 or higher  
- Required libraries:  
  `pandas`, `requests`, `sqlalchemy`, `boto3`, `psycopg2`, `python-dotenv`
- AWS credentials and a weather API key (e.g., OpenWeatherMap)
- (Optional) Redshift database for data storage

---

### 🔧 Installation

# Clone the repository
git clone https://github.com/KarChikey420/weather_analyser.git
cd weather_analyser

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate      # (For Mac/Linux)
venv\Scripts\activate         # (For Windows)

# Install dependencies
pip install -r requirements.txt

weather_analyser/
│
├── Extract_api.py          # Extracts weather data from API
├── transform1.py           # Cleans/transforms the raw data
├── transform.ipynb         # Notebook for data exploration
├── s3_to_redsift.py        # Loads data to AWS S3 & Redshift
├── app.py                  # Web or command-line app entry point
├── main.py                 # Main driver script
├── load.py                 # Additional load utilities
├── .gitignore
└── README.md

# Configuration
Use a .env or config.yaml file for API and AWS credentials:

API_KEY=your_weather_api_key
AWS_ACCESS_KEY=your_aws_access_key
AWS_SECRET_KEY=your_aws_secret_key
AWS_REGION=ap-south-1
S3_BUCKET=my-weather-bucket
REDSHIFT_HOST=my-redshift-host
REDSHIFT_DB=my_database
REDSHIFT_USER=my_user
REDSHIFT_PASSWORD=my_password

# Acknowledgements

Kartikey Negi — Project Creator
AWS & OpenWeatherMap for data and storage services
The Python open-source community for amazing libraries
Inspired by modern Data Engineering ETL architectures
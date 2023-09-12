import logging
from decouple import config
from pyspark.sql import SparkSession, functions as F
import requests, time, json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Spark session
spark = SparkSession.builder.appName("APIDataToSQL").getOrCreate()

# Global variables
HOST = config('HOST')
API_URL = config('API_URL')
GRANT_TYPE = config('GRANT_TYPE')
API_USERNAME = config('API_USERNAME')
API_PASSWORD = config('API_PASSWORD')
API_KEY = config('API_KEY')
TOKEN = None

# Function to refresh the token
def refresh_token():
    global TOKEN
    headers = {
        'Authorization': API_KEY,
        'Content-Type': 'application/json'
    }
    data = {
        'grant_type': GRANT_TYPE,
        'username': API_USERNAME,
        'password': API_PASSWORD
    }
    try:
        response = requests.post(f'{HOST}/oauth/v2/token', headers=headers, json=data)
        response.raise_for_status()
        token_data = response.json()
        TOKEN = token_data.get('access_token')
        logger.info("Token refreshed.")
    except requests.exceptions.RequestException as e:
        logger.error("Failed to refresh token: %s", e)

# SQL Server Configuration
SERVER = config("SERVER")
PORT = config("PORT")
DB = config("DB")
TABLE = "stg.Contracts"
DB_USER = config("DB_USER")
DB_PASSWORD = config("DB_PASSWORD")
SQL_URL = f"jdbc:sqlserver://{SERVER}:{PORT};databaseName={DB};trustServerCertificate=true"
SQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
SQL_PROPERTIES = {"user": DB_USER, "password": DB_PASSWORD, "driver": SQL_DRIVER}


# Function to retrieve existing contract codes from the SQL table
def get_existing_contract_codes():
    try:
        existing_df = spark.read.jdbc(url=SQL_URL, table=TABLE, properties=SQL_PROPERTIES)
        existing_codes = [str(row['code']) for row in existing_df.select('code').collect()]
        return set(existing_codes)
    except Exception as e:
        logger.error("Failed to retrieve existing contract codes: %s", e)
        return set()

# Function to retrieve all data using the refreshed token
def retrieve_all_data():
    refresh_token()  # Refresh the token before making API requests
    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Api-Key': API_KEY
    }
    all_contracts = []  # List to store all contracts

    existing_contract_codes = get_existing_contract_codes()  # Get existing contract codes

    page = 0
    while True:
        try:
            response = requests.get(f"{HOST}{API_URL}?from_page={page}&page_size=100", headers=headers)
            response.raise_for_status()
            api_data = response.json()

            contracts = api_data.get("contracts", [])
            if not contracts:
                break  # No more contracts, break the loop

            for contract in contracts:
                if contract['code'] not in existing_contract_codes:
                    all_contracts.append(contract)
                    existing_contract_codes.add(contract['code'])

            page += 1

            logger.info("Retrieved page %d of contracts", page)
        except requests.exceptions.RequestException as e:
            logger.error("Failed to retrieve data: %s", e)
            break  # Stop the loop on error

    if all_contracts:
        # Convert the list of contracts to a JSON string
        all_contracts_string = json.dumps({"contracts": all_contracts})
        rddjson = spark.sparkContext.parallelize([all_contracts_string])
        df = spark.read.json(rddjson)
        exploded_df = df.select(F.explode(df.contracts).alias('contracts')).select('contracts.*')
        exploded_df = exploded_df.withColumn('retrieved', F.lit(datetime.now()))     

        # Write the dataframe into a SQL table
        exploded_df.write.jdbc(url=SQL_URL, table=TABLE, mode='append', properties=SQL_PROPERTIES)  # Use 'append' mode
        logger.info(f"All new data was written into {TABLE}")

# Main loop
try:
    retrieve_all_data()
    time.sleep(5)  # Adjust sleep time as needed
except KeyboardInterrupt:
    logger.info("Script terminated by user.")

# Stop Spark session
spark.stop()

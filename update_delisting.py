from main   import ProxyRequester
from dotenv import load_dotenv 

import os 
import json 
import requests 
import datetime
import logging 
import datetime


# Setup Logging
logging.basicConfig(
    filename='delisting_update.log', # Set a file for save logger output 
    level=logging.INFO, # Set the logging level
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )
LOGGER = logging.getLogger(__name__)
LOGGER.info("Init Global Variable")


# Setup .env
load_dotenv(override=True)

PROXY = os.getenv('proxy')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')


# Requester api url from main.py
REQUESTER = ProxyRequester(proxy=PROXY)


def process_url() -> str: 
    today = datetime.now()

    # Format the date into the 'YYYYMMDD' format required by the API
    date_str = today.strftime('%Y%m%d')
    LOGGER.info(f"Checking for delistings on date: {date_str}")

    api_url = f"https://www.idx.co.id/primary/ListingActivity/GetIssuedHistory?caType=DELIST&dateFrom={date_str}&dateTo={date_str}&start=0&length=999"
    return api_url

def get_delist_data():
    """ 
    Fetches delisting data from IDX API and returns a dictionary
    with ticker as key and delisting date as value.
    The date is formatted as 'YYYY-MM-DD'.
    """
    api_url = process_url()
    
    try: 
        response = REQUESTER.fetch_url(api_url)
        if response == False:
            raise Exception("Error retrieving active symbols from IDX json.")
        datas = json.loads(response)['data']
    
        # Process the data into a clean dictionary: {'TICKER.JK': 'YYYY-MM-DD'}
        delist_dict = {}
        for data in datas: 
            code_emiten = data.get('KodeEmiten')
            date = data.get('TanggalPencatatan')
            date_clean = date.split("T")[0]
            
            if date and code_emiten:
                # Add the .JK suffix to match with data in db
                code_emiten = code_emiten + ".JK"
                delist_dict[code_emiten] = date_clean
        
        return delist_dict
    
    except requests.exceptions.RequestException as error:
        LOGGER.error(f"Error fetching data from IDX API: {error}")
        return {}
    except json.JSONDecodeError:
        LOGGER.error("Error decoding JSON from IDX response.")
        return {}


def update_delisting_dates_db(delist_data: dict):
    """
    Updates the delisting_date in the Supabase idx_company_profile table.
    Only updates if the delisting_date is currently NULL.

    Args:
        delist_data (dict): A dictionary where keys are ticker symbols
                            and values are delisting dates in 'YYYY-MM-DD' format.
    """

    if not delist_data:
        LOGGER.info("No delist data to process.")
        return

    db_headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal" # Don't need the updated data back
    }

    updates_to_perform = 0
    for ticker, delist_date in delist_data.items():
        try: 
            check_url = f"{SUPABASE_URL}/rest/v1/idx_company_profile?symbol=eq.{ticker}&select=delisting_date"
            check_response = requests.get(check_url, headers=db_headers)
            check_response.raise_for_status()
            
            db_records = check_response.json()
            
            # If the company exists and its delisting_date is empty (null)
            if db_records and db_records[0].get('delisting_date') is None:
                LOGGER.info(f"Found company '{ticker}' with no delisting date. Preparing to update")

                # Perform the update
                update_url = f"{SUPABASE_URL}/rest/v1/idx_company_profile?symbol=eq.{ticker}"
                update_payload = {"delisting_date": delist_date}

                update_response = requests.patch(update_url, headers=db_headers, json=update_payload)
                update_response.raise_for_status()
                
                LOGGER.info(f"Successfully updated delisting date for '{ticker}' to '{delist_date}'.")
                updates_to_perform += 1

            elif not db_records:
                LOGGER.info(f"Warning: Ticker '{ticker}' from IDX was not found in the database. Skipping.")

        except requests.exceptions.RequestException as error:
            LOGGER.error(f"Error processing ticker '{ticker}': {error}")
            if error.response is not None:
                LOGGER.error(f"Response content: {error.response.text}")

    LOGGER.info(f"\nUpdate process finished. Performed {updates_to_perform} updates.")


if __name__ == "__main__":
    delisted_companies = get_delist_data()
    update_delisting_dates_db(delisted_companies)
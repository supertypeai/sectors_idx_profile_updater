import json
import os
from dotenv import load_dotenv
from supabase import create_client
from shareholders_scraper import get_shareholder_data, handle_percentage_duplicate_stringified
import pandas as pd
import logging
from imp import reload
import datetime

load_dotenv()

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('The shareholders data additional scraper program started')

if __name__ == "__main__":
  url_supabase = os.getenv("SUPABASE_URL")
  key = os.getenv("SUPABASE_KEY")
  supabase = create_client(url_supabase, key)

  DATA_DIR = os.path.join(os.getcwd(), "data")
  failed_json_file = os.path.join(DATA_DIR, "failed_data.json")

  file = open(failed_json_file)
  data = json.load(file)

  LOG_FILENAME = 'scrapper.log'
  initiate_logging(LOG_FILENAME)


  symbol_list = []
  if (len(data) > 0):
    # If there are any data to be handled
    for dict_data in data:
      symbol_list.append(dict_data['ticker'])

    get_shareholder_data(symbol_list, supabase, True) # COMMENT OUT THIS ONE TO TEST THE DB UPDATE

    # Preparing to be inserted to db
    CSV_FILE = os.path.join(DATA_DIR, "additional_shareholders_data.csv")
    df_scrapped = handle_percentage_duplicate_stringified(pd.read_csv(CSV_FILE))
    records = df_scrapped.to_dict(orient="records")

    # Update db
    try:
      for record in records:
        supabase.table("idx_company_profile").update(
            {"shareholders": record['shareholders']}
        ).eq("symbol", record['symbol']).execute()
        print(f"Successfully updated shareholders data {record['symbol']}")

      print(
          f"Successfully updated {len(records)} data to database"
      )
    except Exception as e:
      raise Exception(f"Error upserting to database: {e}")
    

    logging.info(f"{datetime.datetime.now().strftime('%Y-%m-%d')} the additional shareholders data has been scrapped.")
    


import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
import ssl
import urllib.request
import os
import time
import json
import logging
from imp import reload
import datetime
from random import choice
import re
import sys
import requests

load_dotenv()

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('The shareholders data scraper program started')


def get_management_data(supabase,symbol):
    id_data = supabase.table("idx_active_company_profile").select("symbol",'directors','comissioners').eq("symbol", symbol).execute()
    id_data = pd.DataFrame(id_data.data)

    # Handling for further stringified json
    if (type(id_data["comissioners"][0]) == str):
      df = pd.concat([pd.DataFrame(json.loads(id_data["comissioners"][0])),pd.DataFrame(json.loads(id_data["directors"][0]))])
    else:
      df = pd.concat([pd.DataFrame(id_data["comissioners"][0]),pd.DataFrame(id_data["directors"][0])])
    return df


USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Mobile/15E148 Safari/604.1'
]

HEADERS = {
        "User-Agent": choice(USER_AGENTS),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

  
PROXY_URL = os.getenv("proxy")
PROXIES = {
    'http': PROXY_URL,
    'https': PROXY_URL,
}
def fetch_url_proxy(url):
  response = requests.get(url, proxies=PROXIES, verify=False)   
  status_code = response.status_code
  if (status_code == 200):
    data = response.json()
    return data
  else:
    print(f"Failed to fetch from {url}. Get status code : {status_code}")
    return None

def fetch_url(url):
  req = urllib.request.Request(url, headers=HEADERS)
  resp = urllib.request.urlopen(req)
  status_code = resp.getcode()
  if (status_code == 200):
    data= resp.read()
    json_data = json.loads(data)
    return json_data
  else:
    print(f"Failed to fetch from {url}. Get status code : {status_code}")
    return None


TRUTH_DICT = {False:'No', True:'Yes'}
SHAREHOLDERS_RENAMING = {
            'Nama':'name',
            'Jabatan':'position',
            'Afiliasi':'affiliated',
            'Independen':'independent',
            'Jumlah':'share_amount',
            'Kategori':'type',
            'Persentase':'share_percentage'
        }

def _change_bool_to_string(list_dict, key_name): 
    for i in range(len(list_dict)):
        list_dict[i][key_name] = TRUTH_DICT.get(list_dict[i][key_name])
    return list_dict

def _clean_dict(list_dict, key_name=None):
    if not list_dict:
        return None
    if key_name :
        list_dict = _change_bool_to_string(list_dict, key_name)
    for dct in list_dict.copy():
        for key in list(dct.keys()):
            dct[SHAREHOLDERS_RENAMING.get(key, key)] = dct.pop(key)
    return list_dict

def get_new_shareholders_data(symbol, supabase):    
    url = f"https://www.idx.co.id/primary/ListedCompany/GetCompanyProfilesDetail?KodeEmiten={symbol}&language=en-us"
    # data = fetch_url(url)
    data = fetch_url_proxy(url)
    
    if (data['ResultCount'] == 0):
      # Case: ResultCount == 0
      return None, None, None
    
    else:
      # Shareholders Data
      shareholders_data = data['PemegangSaham']
      shareholders = [{key: str(value).strip().title() if isinstance(value, str) else value 
                 for key, value in sub.items() if key != 'Pengendali'} 
                for sub in shareholders_data]
      shareholders = _clean_dict(shareholders)
      shareholders_df = pd.DataFrame(shareholders)

      name_mapping = {'Saham Treasury'        : 'Treasury Stock',
                      'Pengendali Saham'      : 'Controlling Shareholder',
                      'Non Pengendali Saham'  : 'Non Controlling Shareholder',
                      'Masyarakat Lainnya (Lx International ( Spore) Pte, Ltd' : "Public (Foreign)",
                      'Masyarakat Pemodal Asing'    : "Public (Foreign)",
                      'Masyarakat Pemodal Nasional' : "Public",
                      'Masyarakat Warkat'           : 'Public',
                      'Masyarakat Non Warkat'       : 'Public',
                      'Masyarakat Umum'             : 'Public',
                      'Masyarakat Dengan Kepemilikan Masing-Masing Kurang Dari 5 Persen' : "Public",
                      "Masyarakat (Masing-Masing)"  : "Public",
                      "Masyarakat (Publik)"         : "Public",
                      "Masyarakat Dibawah 5 %"      : "Public",
                      'Masyarakat': 'Public',
                      'MASYARAKAT': 'Public',
                      'Publik': 'Public',
                      'PUBLIK': 'Public',
                      'Masyarakat Lainnya': 'Other Public',
                      'Negara Republik Indonesia': 'Republic of Indonesia',
                      'NEGARA REPUBLIK INDONESIA': 'Republic of Indonesia',
                      'Pemerintah Negara Republik Indonesia' : 'Republic of Indonesia',
                      'Pemerintah Ri' : 'Republic of Indonesia',
                      'Kejaksaan Agung': 'Attorney General',
                      'KEJAKSAAN AGUNG': 'Attorney General',
                      'Biro klasifikasi indonesia, pt.': 'Pt Biro Klasifikasi Indonesia',
                      'Pt Biro Klasifikasi Indonesia Persero Tbk': 'Pt Biro Klasifikasi Indonesia',
                      'Direksi': 'Director',
                      'AFILIASI PENGENDALI':'Controlling Affiliate',
                      'Pihak Afiliasi ':'Affiliate Parties',
                      'Pihak Afilasi':'Affiliate Parties',
                      'Pt. Asabri (Persero)':'Pt Asabri (Persero)',
                      'Asabri (Persero),Pt':'Pt Asabri (Persero)',
                      'Pt Asabri (Persero) Dapen Polri':'Pt Asabri (Persero)',
                      'Pt Asabri':'Pt Asabri (Persero)',
                      'Pt Asabri - Dapen':'Pt Asabri (Persero)',
                      'Pt Asabri (Persero) - Dapen Tni':'Pt Asabri (Persero)',
                      'Pt Danantara Asset Management (Persero)':'Pt Danantara Asset Management',
                      'Pt Danantara Aset Manajemen (Persero)':'Pt Danantara Asset Management',
                      '0': np.nan,
                      '-': np.nan,
                      '': np.nan}
      shareholders_df = shareholders_df.replace({'name': name_mapping})


      shareholders_df["share_amount"] = shareholders_df["share_amount"].apply(lambda x: int(float(x)))
      shareholders_df["share_percentage"] = shareholders_df["share_percentage"].astype('float')
      shareholders_df = shareholders_df[shareholders_df.share_amount > 0]
      
      df = get_management_data(supabase,f"{symbol}.JK")
      
      if df.shape[0] != 0 :
          shareholders_df = shareholders_df.merge(df,on="name",how="left")

          shareholders_df["position"] = shareholders_df.apply(lambda x: "More Than 5%" if pd.isna(x['position']) and x["share_percentage"] >= 0.05 else x['position'],axis=1)
          shareholders_df["position"] = shareholders_df.apply(lambda x: "Scripless Public Share" if x["name"] == "Public (Scripless)"	else (
                                                              "Scrip Public Share" if x["name"] == "Public (Scrip)" else x['position']),axis=1)
          
          shareholders_df = shareholders_df[["name","position","share_amount","share_percentage"]]

          shareholders_df.rename(columns={"position":"type"},inplace=True)

      else:
          shareholders_df["type"] = shareholders_df.apply(lambda x: "Scripless Public Share" if x["name"] == "Public (Scripless)"	else (
                                                              "Scrip Public Share" if x["name"] == "Public (Scrip)" else x['type']),axis=1)
          shareholders_df = shareholders_df[["name","type","share_amount","share_percentage"]]
      
      if round(shareholders_df['share_percentage'].sum(),0) > 100:
        shareholders_df['share_percentage'] = (shareholders_df['share_amount']/sum(shareholders_df['share_amount']))*100

      shareholders_df = shareholders_df.sort_values("name")

      shareholders_df = shareholders_df.groupby("name").sum().reset_index(drop=False)

      # Directors Data
      try:
        directors_data = data['Direktur']
        directors_processed_data = []
        for i in range(len(directors_data)):
          temp_data = {}
          temp_data['name'] = directors_data[i]['Nama'].title()
          temp_data['position'] = directors_data[i]['Jabatan'].title()
          if ("Vice Presiden" in temp_data['position']):
            temp_data['position'].replace("Vice Presiden", "Vice President")
          temp_data['affiliation'] = directors_data[i]['Afiliasi']
          directors_processed_data.append(temp_data)
        directors_df = pd.DataFrame(directors_processed_data)
      except Exception as e: 
        print(f"Failed to get Directors data. Returning None: {e}")
        directors_df = None
          
      # Commissioners Data
      try:
        commissioners_data = data['Komisaris']
        commissioners_processed_data = []
        for i in range(len(commissioners_data)):
          temp_data = {}
          temp_data['name'] = commissioners_data[i]['Nama'].title()
          temp_data['position'] = commissioners_data[i]['Jabatan'].title()
          if ("Vice Presiden" in temp_data['position']):
            temp_data['position'].replace("Vice Presiden", "Vice President")
          if ("Commisioner" in temp_data['position']):
            temp_data['position'].replace("Commisioner", "Commissioner")
          temp_data['independent'] = commissioners_data[i]['Independen']
          commissioners_processed_data.append(temp_data)
        commissioners_df = pd.DataFrame(commissioners_processed_data)
      except:
        print(f"Failed to get Commissioners data. Returning None: {e}")
        commissioners_df = None
        
      return shareholders_df, directors_df, commissioners_df
    
MAX_ATTEMPT = 3
SLEEP = 1.5
CWD = os.getcwd()
DATA_DIR = os.path.join(CWD, "data")


def get_shareholder_data(symbol_list: list, supabase, is_failure_handling = False):
  retry = 0 
  i = 0 
  failed_list = []
  data = pd.DataFrame(columns=['symbol', 'shareholders'])

  while len(symbol_list) > 0:
    ticker = symbol_list[0]
    try:
      print(f"Trying to get data from {ticker}")
      shareholders_df, directors_df, commissioners_df = get_new_shareholders_data(ticker, supabase) # This function includes search for directors and commissioners
      
      # Check for shareholders
      if (shareholders_df is not None):
        shareholders_records = shareholders_df.to_json(orient='records')

        # Check for directors
        directors_records = directors_df.to_json(orient="records") if directors_df is not None else None
        if (directors_records is None): print(f"[NONE VALUE] None value detected for Directors data from {ticker}")
        # Check for commisioners
        commissioners_records = commissioners_df.to_json(orient="records") if commissioners_df is not None else None
        if (commissioners_records is None): print(f"[NONE VALUE]None value detected for Commissioners data from {ticker}")


        data = pd.concat([data ,pd.DataFrame(data={'symbol':f"{ticker}.JK", 'shareholders':[shareholders_records], 'directors' : [directors_records], 'commissioners' : [commissioners_records]})])
        print(f"Successfully get data from {ticker}")
      else:
        print(f"[NONE VALUE] None value detected for Shareholders data from {ticker}")
        failed_list.append({
            "ticker" : ticker,
            "reason" : "None value detected"
            })
      symbol_list.remove(ticker)
      i += 1
      print(f"Finished getting data from {ticker} and Removed {ticker}, this is ticker number {i}")
      print("-------------------------------------------------------------------------------")
      
    except Exception as e :
      print(f"Failed to get the data: {e}")
      retry += 1
      if retry == MAX_ATTEMPT:
          failed_list.append({
            "ticker" : ticker,
            "reason" : "Failed after maximum attempts"
            })
          symbol_list.remove(ticker)
          retry = 0 
          print(f"Failed to get data from {ticker} after {MAX_ATTEMPT} attempts")
          print("-------------------------------------------------------------------------------")
      else:
          print(f"Failed to get data from {ticker} on attempt {retry}. Retrying after {SLEEP} seconds...")
          print("-------------------------------------------------------------------------------")

    time.sleep(SLEEP)

  # Save the data
  if (not is_failure_handling):
    filename = os.path.join(DATA_DIR, f"shareholders_data.csv")
  else:
    filename = os.path.join(DATA_DIR, f"additional_shareholders_data.csv")
  data.to_csv(filename, index=False)

  # Store failed data
  failed_filename = os.path.join(DATA_DIR, f"failed_data.json")
  with open(failed_filename, "w") as final:
    json.dump(failed_list, final, indent=2)

def is_same_dict(dict1: dict, dict2: dict) -> bool :
  for key, val in dict1.items():
    if (key in dict2 and val == dict2[key]):
      continue
    else:
      return False
  return True

def is_dict_in_list(dict_arg : dict, list_arg: list) -> bool:
  for dict_itr in list_arg:
    if (is_same_dict(dict_arg, dict_itr)):
      return True
  return False

def handle_percentage_duplicate_stringified(df: pd.DataFrame):
  for index, row in df.iterrows():
    shareholder_list_data = row['shareholders']
    shareholder_list = json.loads(shareholder_list_data)

    # Handle percentage
    for i in range(len(shareholder_list)):
      shareholder_dict = shareholder_list[i]
      shareholder_dict['share_percentage'] = round(shareholder_dict['share_percentage'] / 100, 5) # Make it 5 digits decimal
      shareholder_list[i] = shareholder_dict

    # Handle duplicate
    new_shareholder_list = [i for n, i in enumerate(shareholder_list) if not is_dict_in_list(i, shareholder_list[:n])]

    # Handle stringified
    director_list = json.loads(row['directors'])
    commissioner_list = json.loads(row['commissioners'])

    df.at[index, 'shareholders'] = new_shareholder_list
    df.at[index, 'directors'] = director_list
    df.at[index, 'commissioners'] = commissioner_list
  return df

if __name__ == "__main__":
  url_supabase = os.getenv("SUPABASE_URL")
  key = os.getenv("SUPABASE_KEY")
  supabase = create_client(url_supabase, key)

  # Preparing to scrape
  symbol = supabase.table("idx_active_company_profile").select("symbol").execute()
  symbol = pd.DataFrame(symbol.data).symbol.str.split(".",expand=True)
  symbol.columns = ["symbol","exchange"]
  symbol = list(symbol.symbol)

  LOG_FILENAME = 'scrapper.log'
  initiate_logging(LOG_FILENAME)

  # Start time
  start = time.time()

  # Check the argument, by default get the first quarter
  # Split the argument to 4 batches
  # First quarter => arg = 0-1
  # Second quarter => arg = 1-2
  # Third quarter => arg = 2-3
  # Fourth quarter => arg = 3-4
  arg = None
  if (len(sys.argv) == 1):
     arg = "0-1"
  else:
     arg = sys.argv[1]

  try:
    args = arg.split("-")
    lower_bound = int(args[0])
    upper_bound = int(args[1])
    quarter_length = (len(symbol)// 4) + 1

    start_idx = lower_bound * quarter_length
    end_idx = min(len(symbol), upper_bound * quarter_length)
    print(f"[FETCHING DATA] Scraping batch {upper_bound} from index {start_idx} to index {end_idx}")
    logging.info(f"Scraping batch {upper_bound} from index {start_idx} to index {end_idx}")

    get_shareholder_data(symbol[start_idx:end_idx], supabase) 

    # Checkpoint
    checkpoint = time.time()

    CSV_FILE = os.path.join(DATA_DIR, "shareholders_data.csv")
    df_scrapped = handle_percentage_duplicate_stringified(pd.read_csv(CSV_FILE))
    records = df_scrapped.to_dict(orient="records")

    # Update db
    try:
      for record in records:
        supabase.table("idx_company_profile").update(
            {"shareholders": record['shareholders'], 
             "directors": record['directors'], 
             "commissioners": record['commissioners']}
        ).eq("symbol", record['symbol']).execute()
        print(f"Successfully updated shareholders data {record['symbol']}")


      print(
          f"Successfully updated {len(records)} data to database"
      )
    except Exception as e:
      raise Exception(f"Error upserting to database: {e}")
    
    # End
    end = time.time()

    print(f"Time elapsed for scraping : {time.strftime('%H:%M:%S', time.gmtime(int(checkpoint - start)))}")
    print(f"Time elapsed to update the database : {time.strftime('%H:%M:%S', time.gmtime(int(end - checkpoint)))}")

    logging.info(f"{datetime.datetime.now().strftime('%Y-%m-%d')} the shareholders data has been scrapped. Execution time: {time.strftime('%H:%M:%S', time.gmtime(end-start))}")

  except Exception as e:
    print(f"[ERROR] Invalid inputted argument : {e}")


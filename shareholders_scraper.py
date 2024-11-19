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

load_dotenv()

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('The shareholders data scraper program started')


def get_management_data(supabase,symbol):
    id_data = supabase.table("idx_active_company_profile").select("symbol",'directors','comissioners').eq("symbol", symbol).execute()
    id_data = pd.DataFrame(id_data.data)
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

'''

'''

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
    data = fetch_url(url)
    
    if (data['ResultCount'] == 0):
      # Case: ResultCount == 0
      return None
    
    else:
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
                      'Kejaksaan Agung': 'Attorney General',
                      'KEJAKSAAN AGUNG': 'Attorney General',
                      'Direksi': 'Director',
                      'AFILIASI PENGENDALI':'Controlling Affiliate',
                      'Pihak Afiliasi ':'Affiliate Parties',
                      'Pihak Afilasi':'Affiliate Parties',
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

      shareholders_df = shareholders_df.sort_values("name")

      shareholders_df = shareholders_df.groupby("name").sum().reset_index(drop=False)
        
      return shareholders_df
    
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
      shareholders_df = get_new_shareholders_data(ticker,supabase)
      if (shareholders_df is not None):
        records = shareholders_df.to_json(orient='records')
        data = pd.concat([data ,pd.DataFrame(data={'symbol':f"{ticker}.JK", 'shareholders':[records]})])
        print(f"Successfully get Shareholders data from {ticker}")
      else:
        print(f"None value detected for Shareholders data from {ticker}")
        failed_list.append({
            "ticker" : ticker,
            "reason" : "None value detected"
            })
      symbol_list.remove(ticker)
      i += 1
      print(f"Finished get Shareholders data from {ticker} and Removed {ticker}, this is ticker number {i}")
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
          print(f"Failed to get Shareholders data from {ticker} after {MAX_ATTEMPT} attempts")
          print("-------------------------------------------------------------------------------")
      else:
          print(f"Failed to get Shareholders data from {ticker} on attempt {retry}. Retrying after {SLEEP} seconds...")
          print("-------------------------------------------------------------------------------")

    time.sleep(SLEEP)

  # Save the data
  if (not is_failure_handling):
    filename = os.path.join(DATA_DIR, f"shareholders_data.csv")
    data.to_csv(filename, index=False)
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

def handle_percentage_and_duplicate(df: pd.DataFrame):
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

    df.at[index, 'shareholders'] = new_shareholder_list
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

  length_list = len(symbol)
  i1 = int(length_list / 2)

  get_shareholder_data(symbol, supabase) # COMMENT OUT THIS ONE TO TEST THE DB UPDATE

  # Checkpoint
  checkpoint = time.time()

  CSV_FILE = os.path.join(DATA_DIR, "shareholders_data.csv")
  df_scrapped = handle_percentage_and_duplicate(pd.read_csv(CSV_FILE))
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
  
  # End
  end = time.time()

  print(f"Time elapsed for scraping : {time.strftime('%H:%M:%S', time.gmtime(int(checkpoint - start)))}")
  print(f"Time elapsed to update the database : {time.strftime('%H:%M:%S', time.gmtime(int(end - checkpoint)))}")

  logging.info(f"{datetime.datetime.now().strftime('%Y-%m-%d')} the shareholders data has been scrapped. Execution time: {time.strftime('%H:%M:%S', time.gmtime(end-start))}")



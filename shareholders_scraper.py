import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
import ssl
import urllib.request
import os
import time
import json
# from multiprocessing import Process

load_dotenv()


def get_management_data(supabase,symbol):
    id_data = supabase.table("idx_active_company_profile").select("symbol",'directors','comissioners').eq("symbol", symbol).execute()
    id_data = pd.DataFrame(id_data.data)
    df = pd.concat([pd.DataFrame(id_data["comissioners"][0]),pd.DataFrame(id_data["directors"][0])])
    return df

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
HEADERS = {
        "User-Agent": USER_AGENT,
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
      shareholders = [{key: str(value).strip() for key, value in sub.items() if key!='Pengendali'} for sub in shareholders_data]
      shareholders = _clean_dict(shareholders)
      shareholders_df = pd.DataFrame(shareholders)

      name_mapping = {'Saham Treasury': 'Treasury Stock',
                      'Pengendali Saham': 'Controlling Shareholder',
                      'Non Pengendali Saham': 'Non Controlling Shareholder',
                      'Masyarakat Warkat': 'Public (Scrip)',
                      'Masyarakat Non Warkat': 'Public (Scripless)',
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
      return shareholders_df
    
MAX_ATTEMPT = 3
SLEEP = 3
CWD = os.getcwd()
DATA_DIR = os.path.join(CWD, "data")

def get_shareholder_data(symbol_list: list, process: int, supabase):
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
  filename = os.path.join(DATA_DIR, f"shareholders_data_P{process}.csv")
  data.to_csv(filename,index=False)

  # Store failed data
  failed_filename = os.path.join(DATA_DIR, f"failed_data_P{process}.json")
  with open(failed_filename, "w") as final:
    json.dump(failed_list, final, indent=2)


if __name__ == "__main__":
  url_supabase = os.getenv("SUPABASE_URL")
  key = os.getenv("SUPABASE_KEY")
  supabase = create_client(url_supabase, key)

  # Preparing to scrape
  symbol = supabase.table("idx_active_company_profile").select("symbol").execute()
  symbol = pd.DataFrame(symbol.data).symbol.str.split(".",expand=True)
  symbol.columns = ["symbol","exchange"]
  symbol = list(symbol.symbol)

  length_list = len(symbol)
  i1 = int(length_list / 2)

  get_shareholder_data(symbol, 1, supabase)


  # p1 = Process(target=get_shareholder_data, args=(symbol[:i1], 1, supabase))
  # p2 = Process(target=get_shareholder_data, args=(symbol[i1:], 2, supabase))

  # p1.start()
  # p2.start()

  # p1.join()
  # p2.join()


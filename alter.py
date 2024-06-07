# PROOF BRIGHTDATA TEMBUS

import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()
import os
from supabase import create_client
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import urllib.request

proxy = os.environ.get("proxy")

proxy_support = urllib.request.ProxyHandler({'http': proxy,'https': proxy})
opener = urllib.request.build_opener(proxy_support)
urllib.request.install_opener(opener)

def retrieve_active_symbols():
    url = "https://www.idx.co.id/primary/StockData/GetSecuritiesStock?start=0&length=9999&code=&sector=&board=&language=en-us"
    with urllib.request.urlopen(url) as response:
        html = response.read()
    data = json.loads(html)
    active_symbols = [entry['Code']+'.JK' for entry in data['data']]
    print(len(active_symbols), 'active symbols')

    return active_symbols

# def retrieve_company_profile(symbol):
#     url = f"https://www.idx.co.id/primary/ListedCompany/GetCompanyProfilesDetail?KodeEmiten={symbol}&language=en-us"
#     with urllib.request.urlopen(url) as response:
#         html = response.read()
#     data = json.loads(html)
#     print(data)

#     return data

retrieve_active_symbols()
# retrieve_company_profile('BBCA')



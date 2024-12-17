import numpy as np
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyrate_limiter import Duration, Limiter, RequestRate
from ratelimit import limits
from requests import Session
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from supabase import create_client
import ssl
import urllib.request
import os
import json
import time
import argparse
from fuzzywuzzy import fuzz
import logging
from imp import reload

all_columns = [
    "symbol",
    "company_name",
    "address",
    "email",
    "phone",
    "fax",
    "NPWP",
    "website",
    "listing_date",
    "listing_board",
    "sub_sector_id",
    "industry",
    "sub_industry",
    "register",
    "shareholders",
    "directors",
    "commissioners",
    "audit_committees",
    "delisting_date",
    "nologo",
    "yf_currency",
    "wsj_format",
    "current_source",
    "updated_on",
    "alias"
]

sub_sector_id_map = {
    "Transportation Infrastructure": 28,
    "Food & Beverage": 2,
    "Holding & Investment Companies": 21,
    "Leisure Goods": 12,
    "Software & IT Services": 30,
    "Basic Materials": 8,
    "Automobiles & Components": 10,
    "Retailing": 14,
    "Investment Service": 22,
    "Consumer Services": 11,
    "Media & Entertainment": 13,
    "Telecommunication": 6,
    "Technology Hardware & Equipment": 31,
    "Banks": 19,
    "Pharmaceuticals & Health Care Research": 24,
    "Household Goods": 1,
    "Tobacco": 3,
    "Insurance": 4,
    "Industrial Goods": 5,
    "Properties & Real Estate": 7,
    "Apparel & Luxury Goods": 9,
    "Food & Staples Retailing": 15,
    "Nondurable Household Products": 16,
    "Alternative Energy": 17,
    "Oil, Gas & Coal": 18,
    "Financing Service": 20,
    "Healthcare Equipment & Providers": 23,
    "Multi-sector Holdings": 26,
    "Heavy Constructions & Civil Engineering": 27,
    "Industrial Services": 25,
    "Utilities": 29,
    "Logistics & Deliveries": 32,
    "Transportation": 33,
}

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program started')

class ProxyRequester:
    def __init__(self, proxy=None):
        """Initializes the ProxyRequester class with the provided proxy

        Args:
            proxy (str, optional): the proxy to be used. Defaults to None. Example: 'brd-customer-xxx-zone-xxx:xxx@brd.superproxy.io:xxx'
        """
        # Set up SSL context to unverified
        ssl._create_default_https_context = ssl._create_unverified_context

        proxy_support = urllib.request.ProxyHandler({'http': proxy,'https': proxy})
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)

    def fetch_url(self, url):
        # Use the installed opener to fetch the URL
        try:
            with urllib.request.urlopen(url) as response:
                return response.read().decode()
        except Exception as e:
            print(f"Error fetching URL: {e}")
            return False

class LimiterSession(LimiterMixin, Session):
    def __init__(self):
        super().__init__(
            limiter=Limiter(
                RequestRate(2, Duration.SECOND * 5)
            ),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
        )

class OwnershipCleaner:
    def __init__(self) -> None:
        """Initializes the OwnershipCleaner class with the current shareholders data

        Args:
            shareholders_df (pd.DataFrame): the dataframe containing the current shareholders data
        """
        # if not shareholders_df.empty:
        #     self.current_shareholders_data = self._convert_json_col_to_df(shareholders_df, 'shareholders')[['symbol','name','share_percentage']]
        #     self.current_shareholders_data['name_lower'] = self.current_shareholders_data['name'].str.lower()
        #     self.current_shareholders_data = self.current_shareholders_data.drop('name', axis=1)
        # else:
        #     columns = ['symbol','name_lower','share_percentage']
        #     self.current_shareholders_data = pd.DataFrame(columns=columns)

    def _convert_json_col_to_df(self, df, col_name):
        """Converts a json column in a dataframe to a new dataframe

        Args:
            df (pd.DataFrame): the original dataframe
            col_name (str): the name of the json column

        Returns:
            pd.DataFrame: the converted dataframe
        """
        if df.empty:
            return None
        
        temp_df = df.loc[df[col_name].notna(),['symbol', col_name]].set_index('symbol')

        try:
            temp_df[col_name] = temp_df[col_name].apply(json.loads)
        except TypeError as e:
            pass
        
        temp_df = temp_df.explode(col_name)
        temp_df = temp_df[col_name].apply(pd.Series, dtype='object')

        temp_df = temp_df.reset_index()
        temp_df.columns = temp_df.columns.str.lower()
        temp_df = temp_df.dropna(axis=1, how='all')
        return temp_df

    def _process_management_col_to_df(self, df, col_name):
        """Processes the management column (directors, commissioners, or audit_committees) in the dataframe to a new dataframe

        Args:
            df (pd.DataFrame): the original dataframe
            col_name (str): the name of the management column

        Returns:
            pd.DataFrame: the processed dataframe
        """
        temp_df = self._convert_json_col_to_df(df, col_name)
        temp_df = temp_df.dropna(subset=['name', 'position'])
        temp_df['position'] = temp_df['position'].str.title()
        temp_df['name'] = temp_df['name'].str.title()
        
        position_renaming_dicts = \
        {
            'directors':{'Vice President': 'Vice President Director',
                        'Vice Presiden Director': 'Vice President Director',
                        'Presiden Direktur': 'President Director',
                        'Wakil Presiden Direktur': 'Vice President Director', 
                        'Direktur': 'Director',
                        'Direktur Utama': 'President Director',
                        'Wakil Direktur Utama': 'Vice President Director',
                        '': 'Director'},
            
            'commissioners':{'President Commisioner': 'President Commissioner',
                            'Vice President Commisioner': 'Vice President Commissioner',
                            'Presiden Komisaris': 'President Commissioner',
                            'Komisaris': 'Commissioner',
                            'Wakil Komisaris Utama': 'Vice President Commissioner',
                            'Komisaris Utama': 'President Commissioner',
                            'Wakil Presiden Komisaris': 'Vice President Commissioner',
                            '': 'Commissioner'},
            
            'audit_committees':{'Ketua': 'Head of Audit Committee',
                            'Anggota': 'Member of Audit Committee',
                            'Ketua Komite Audit': 'Head of Audit Committee',
                            'Anggota Komite Audit': 'Member of Audit Committee',
                            'Head': 'Head of Audit Committee',
                            'Member': 'Member of Audit Committee',
                            '': 'Audit Committee'}
            }
        
        temp_df['position'] = temp_df['position'].replace(position_renaming_dicts[col_name])
        temp_df = temp_df.drop_duplicates(subset=['symbol', 'name', 'position'])
        
        return temp_df
        
    def _process_shareholder_col_to_df(self, df, col_name):
        """Processes the shareholder column in the dataframe to a new dataframe
 
        Args:
            df (pd.DataFrame): the original dataframe
            col_name (str): the name of the shareholder column

        Returns:
            pd.DataFrame: the processed dataframe
        """
        def convert_share_percentage(x):
            try:
                return round(float(x.replace('%', '')) / 100, 4)
            except Exception as e:
                print(f'Error: {e}. Value is {x}')
                return None
        
        def convert_share_amount(x):
            try:
                return float(x.replace(',', ''))
            except Exception as e:
                print(f'Error: {e}. Value is {x}')
                return None
        
        shareholders_df = self._convert_json_col_to_df(df, col_name)
        shareholders_df = shareholders_df.drop_duplicates()
        
        old_cols = ['share_amount', 'share_percentage']
        new_cols = ['share_amount_new', 'share_percentage_new']
        
        for cols in old_cols:
            if cols not in shareholders_df.columns:
                shareholders_df[cols] = None
        
        old_shareholders_df = shareholders_df[shareholders_df['share_amount'].notna()].copy()
        old_shareholders_df = old_shareholders_df.drop(columns=new_cols)

        shareholders_df = shareholders_df.drop(index=old_shareholders_df.index)
        shareholders_df = shareholders_df.drop(columns=old_cols)
        
        shareholders_df[['share_amount_new', 'share_percentage_new']] = shareholders_df[['share_amount_new', 'share_percentage_new']].astype(str)
        # shareholders_df['share_percentage_new'] = shareholders_df['share_percentage_new'].apply(lambda x: round(float(x.replace('%',''))/100,4))
        shareholders_df['share_percentage_new'] = shareholders_df['share_percentage_new'].apply(convert_share_percentage)
        # shareholders_df['share_amount_new'] = shareholders_df['share_amount_new'].apply(lambda x: float(x.replace(',','')))
        shareholders_df['share_amount_new'] = shareholders_df['share_amount_new'].apply(convert_share_amount)

        shareholders_df.loc[shareholders_df['name'] == 'Saham Treasury', 'type'] = 'Treasury Stock'
        
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
        shareholders_df = shareholders_df.loc[shareholders_df['share_amount_new']>0]
        
        type_mapping = {
            'Direksi':'Director',
            'Commisioner':'Commissioner',
            'Komisaris':'Commissioner',
            'Kurang dari 5%':'Less Than 5%',
            'Lebih dari 5%':'More Than 5%',
            'Saham Pengendali': 'Controlling Share',
            'Saham Non Pengendali': 'Non Controlling Share',
            'Masyarakat Warkat': 'Scrip Public Share',
            'Masyarakat Non Warkat': 'Scripless Public Share',
            '': '-'
        }
        
        shareholders_df = shareholders_df.replace({'type': type_mapping})
        shareholders_df['name'] = shareholders_df['name'].str.title()
        
        directors_df = self._process_management_col_to_df(df, 'directors')
        directors_df = directors_df.drop_duplicates(subset=['symbol', 'name'])
        directors_df = directors_df.query("name != '-'")
        directors_df['name_lower'] = directors_df['name'].str.lower()

        commissioners_df = self._process_management_col_to_df(df, 'commissioners')
        commissioners_df = commissioners_df.drop_duplicates(subset=['symbol', 'name'])
        commissioners_df = commissioners_df.query("name != '-'")
        commissioners_df['name_lower'] = commissioners_df['name'].str.lower()
        
        shareholders_df['name_lower'] = shareholders_df['name'].str.lower()
        merged_df = pd.merge(shareholders_df, directors_df[['symbol', 'name_lower', 'position']], left_on=['symbol','name_lower'], right_on=['symbol','name_lower'], how='left')
        merged_df = pd.merge(merged_df, commissioners_df[['symbol', 'name_lower', 'position']], left_on=['symbol','name_lower'], right_on=['symbol','name_lower'], how='left', suffixes=['_dir','_comm'])
        
        merged_df['type'] = np.where(merged_df['position_dir'].notna(), merged_df['position_dir'], merged_df['type'])
        merged_df['type'] = np.where(merged_df['position_comm'].notna() & merged_df['position_dir'].isna(), merged_df['position_comm'], merged_df['type'])
        merged_df = merged_df.groupby(['symbol', 'name_lower', 'type']).agg({'name':'first', 'share_amount_new':'sum', 'share_percentage_new':'sum'}).reset_index()
        
        merged_df = merged_df.drop(columns=['name_lower'])
        
        merged_df = merged_df.rename(columns={'share_amount_new':'share_amount', 'share_percentage_new':'share_percentage'})
        merged_df = pd.concat([merged_df, old_shareholders_df], ignore_index=True)
        
        
        return merged_df
    
    def process_ownership_col(self, df, col_name):
        """
        Process the ownership column (directors, commissioners, audit_committees or shareholders) in a dataframe to a json format
        Args:
            df (pd.DataFrame): dataframe to be processed
            col_name (str): column name to be processed
        Returns:
            pd.DataFrame: processed dataframe containing the ownership column in json format
        """
        if col_name in ['directors', 'commissioners', 'audit_committees']:
            temp_df = self._process_management_col_to_df(df, col_name)
        elif col_name == 'shareholders':
            temp_df = self._process_shareholder_col_to_df(df, col_name)

        temp_df = temp_df.replace(np.nan, None)
        json_df = temp_df.groupby('symbol').apply(lambda x: x.drop(columns=['symbol']).to_json(orient='records')).reset_index(name=col_name)
        json_df[col_name] = json_df.apply(lambda x: json.loads(x[col_name]), axis=1)
    
        return json_df

class IdxProfileUpdater:
    def __init__(
        self,
        company_profile_csv_path=None,
        supabase_client=None,
        proxy=None
    ):
        """Class to update idx_company_profile table in supabase database.
        Args:
            company_profile_csv_path (str, optional): Path to the CSV file containing company profile data.
            supabase_client (Client, optional): Supabase client object for database interactions.
            proxy (str, optional): Proxy settings for web requests.
        """

        if company_profile_csv_path and supabase_client:
            raise ValueError(
                "Only one of company_profile_csv_path or supabase_client should be provided."
            )

        elif company_profile_csv_path:
            self.supabase_client = None
            self.current_data = pd.read_csv(company_profile_csv_path, usecols=all_columns)


        elif supabase_client:
            response = (
                supabase_client.table("idx_company_profile_duplicate").select("*").execute()
            )
            self.supabase_client = supabase_client
            self.current_data = pd.DataFrame(response.data, columns=all_columns)

        else:
            self.supabase_client = None
            self.current_data = pd.DataFrame(columns=all_columns)

        self.new_data = None
        self.updated_rows = None
        self.modified_symbols = set()
        self.ownershipcleaner = OwnershipCleaner()
        self._session = LimiterSession()
        self._requester = ProxyRequester(proxy)

    def _retrieve_active_symbols(self):
        url = "https://www.idx.co.id/primary/StockData/GetSecuritiesStock?start=0&length=9999&code=&sector=&board=&language=en-us"
        response = self._requester.fetch_url(url)
        if response == False:
            raise Exception("Error retrieving active symbols from IDX json.")
        data = json.loads(response)['data']
        active_symbols = {index['Code'] + '.JK': index['Name'] for index in data}
        print(len(active_symbols), 'active symbols')

        return active_symbols
    
    @limits(calls=2, period=4)
    def _retrieve_idx_profile(self, yf_symbol):
        symbol = (yf_symbol.split(".")[0]).lower()
        url = f"https://www.idx.co.id/primary/ListedCompany/GetCompanyProfilesDetail?KodeEmiten={symbol}&language=en-us"
        response = self._requester.fetch_url(url)
        if response == False:
            raise Exception("Error retrieving active symbols from IDX json.")
        
        data = json.loads(response)
        profile_dict = {"symbol": yf_symbol}
        profiles = data['Profiles'][0]

        key_renaming = {
            "Alamat": "address",
            "BAE": "register",
            "Industri": "industry",
            "SubIndustri": "sub_industry",
            "Email": "email",
            "Fax": "fax",
            "NamaEmiten": "company_name",
            "PapanPencatatan": "listing_board",
            "TanggalPencatatan": "listing_date",
            "Telepon": "phone",
            "Website": "website",
            "NPWP": "NPWP",
        }
        
        shareholders_renaming = {
            'Nama':'name',
            'Jabatan':'position',
            'Afiliasi':'affiliated',
            'Independen':'independent',
            'Jumlah':'share_amount_new',
            'Kategori':'type',
            'Persentase':'share_percentage_new'
        }
        
        truth_dict = {False:'No', True:'Yes'}
        
        for key, value in profiles.items():
            if key.lower() == "subsektor":
                profile_dict["sub_sector_id"] = sub_sector_id_map.get(value, None)
            elif key in key_renaming.keys():
                renamed_key = key_renaming.get(key, key)
                profile_dict[renamed_key] = str(value).strip()
                
        def _change_bool_to_string(list_dict, key_name): 
            for i in range(len(list_dict)):
                list_dict[i][key_name] = truth_dict.get(list_dict[i][key_name])
            return list_dict
        
        def _clean_dict(list_dict, key_name=None):
            if not list_dict:
                return None
            if key_name :
                list_dict = _change_bool_to_string(list_dict, key_name)
            for dct in list_dict.copy():
                for key in list(dct.keys()):
                    dct[shareholders_renaming.get(key, key)] = dct.pop(key)
            return list_dict
        
        directors = data['Direktur']
        directors = _clean_dict(directors, 'Afiliasi')
        profile_dict['directors'] = directors
        
        commissioners = data['Komisaris']
        commissioners = _clean_dict(commissioners, 'Independen')
        profile_dict['commissioners'] = commissioners
        
        audit_committees = data['KomiteAudit']
        audit_committees = _clean_dict(audit_committees)
        profile_dict['audit_committees'] = audit_committees
        
        shareholders_data = data['PemegangSaham']
        shareholders = [{key: str(value).strip() for key, value in sub.items() if key!='Pengendali'} for sub in shareholders_data]
        shareholders = _clean_dict(shareholders)
        profile_dict['shareholders'] = shareholders
        
        profile_dict["delisting_date"] = None
        
        return profile_dict
    
    def update_company_profile_data(self, update_new_symbols_only=True, target_symbols=None):
        """Update company profile data.

        Args:
            update_new_symbols_only (bool, optional): Whether to update only rows with new symbols or all rows. Defaults to True.
        """

        def update_profile_for_row(row):
            temp_row = row.copy()
            use_selenium = False
            profile_dict = self._retrieve_idx_profile(row["symbol"])
            for key in profile_dict.keys():
                temp_row[key] = profile_dict[key]
            print('new data',profile_dict)
            time.sleep(3)

            # replace '-','0','' with None
            replace_cols = ['address', 'email', 'phone', 'fax', 'NPWP', 'website', 'register']
            temp_row[replace_cols] = temp_row[replace_cols].replace(['-', '0', ''], [None, None, None])
                
            temp_row["updated_on"] = pd.Timestamp.now(tz="GMT").strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            
            if not pd.isna(row["company_name"]) and temp_row["company_name"] != row["company_name"]:
                print("isna")
                print(f"Company name updated for {temp_row['symbol']}.")
                print(f"Old name: {row['company_name']}")
                temp_row["alias"] = row["alias"].append(row["company_name"])
                
            return temp_row
                    
        ### Management & Shareholders Cleaning
        def clean_ownership(df, columns):
            profile_df = df.copy()
            merged_updated_df = pd.DataFrame()
            
            for col_name in columns:
                temp_df = self.ownershipcleaner.process_ownership_col(profile_df, col_name)
                if merged_updated_df.empty:
                    merged_updated_df = temp_df.copy()
                else:
                    merged_updated_df = pd.merge(merged_updated_df, temp_df, on="symbol", how="outer")    
            return merged_updated_df
            
        try: 
            retrieved_active_company = self._retrieve_active_symbols()
            retrieved_active_symbols = [symbol for symbol in retrieved_active_company]
            print(retrieved_active_symbols)
        except Exception as e:   
            print(e)
        
        company_profile_data = self.current_data.copy()
        table_active_symbols = company_profile_data.query("delisting_date.isnull()")[
            "symbol"
        ].unique()
        updated_inactive_symbols = list(set(table_active_symbols) - set(retrieved_active_symbols))
        updated_new_symbols = list(set(retrieved_active_symbols) - set(table_active_symbols))

        updated_inactive_filter = company_profile_data.query(
            "symbol in @updated_inactive_symbols"
        ).index
        company_profile_data.loc[
            updated_inactive_filter, "delisting_date"
        ] = pd.Timestamp.now().strftime("%Y-%m-%d")
        self.modified_symbols.update(updated_inactive_symbols)

        company_profile_data = pd.concat(
            [company_profile_data, pd.DataFrame({"symbol": updated_new_symbols})],
            ignore_index=True,
        )

        if update_new_symbols_only:
            if not target_symbols:
                target_symbols = updated_new_symbols
                
            updated_company_name_symbols = []

            with open('bypass-symbols.json') as bypass_file:
                bypass_symbols = json.load(bypass_file).get("symbols", [])
            for _, row in company_profile_data.iterrows():
                if row["symbol"] in bypass_symbols:
                    continue
                try:
                    similarity = fuzz.ratio(row['company_name'][0:30].lower(), retrieved_active_company[row['symbol']].lower())
                    if similarity < 65:
                        updated_company_name_symbols.append(row['symbol'])
                except Exception as e:
                    pass   
            
            print("Possible updated company name: ", updated_company_name_symbols)
            
            updated_new_filter = company_profile_data.query(
                "symbol in @updated_new_symbols and symbol in @target_symbols"
            ).index
            
            updated_new_filter = updated_new_filter.union(company_profile_data.query(
                "symbol in @updated_company_name_symbols"
            ).index)
            
            rows_to_update = company_profile_data.loc[updated_new_filter].copy()

        else:
            if not target_symbols:
                target_symbols = retrieved_active_symbols
            active_filter = company_profile_data.query(
                "symbol in @retrieved_active_symbols and symbol in @target_symbols"
            ).index
            rows_to_update = company_profile_data.loc[active_filter].copy()
        
        if rows_to_update.empty:
            print("No rows to update.")
            return
        
        self.modified_symbols.update(rows_to_update["symbol"].tolist())
        rows_to_update = rows_to_update.apply(update_profile_for_row, axis=1)

        self._rows_to_update_temp = rows_to_update.copy()
        
        columns_to_clean = [
                "shareholders",
                "directors",
                "commissioners",
                "audit_committees",
            ]
        
        try:
            cleaned_rows = clean_ownership(rows_to_update, columns_to_clean)
        
        except Exception as e:
            print(f'Failed to clean ownership columns. Dropping uncleaned columns for upsert and saving them to csv instead. Error message: {e}')
            rows_to_update[columns_to_clean] = rows_to_update[columns_to_clean].applymap(json.dumps)
            rows_to_update[["symbol"] + columns_to_clean].to_csv('ownership_data_uncleaned.csv', index=False)
            rows_to_update = rows_to_update.drop(columns=columns_to_clean)
            
        else:
            rows_to_update.set_index('symbol', inplace=True)
            rows_to_update.update(cleaned_rows.set_index('symbol'))
            rows_to_update.reset_index(inplace=True)

        company_profile_data.set_index('symbol', inplace=True)
        company_profile_data.update(rows_to_update.set_index('symbol'))
        company_profile_data.reset_index(inplace=True)

        self.new_data = company_profile_data
        self.updated_rows = self.new_data.query("symbol in @self.modified_symbols")
        

    def save_update_to_csv(self, updated_rows_only=True):
        """Generate CSV file containing updated data.

        Args:
            updated_rows_only (bool, optional): Whether to save only updated rows or all rows. Defaults to True.
        """
        if self.new_data is None:
            raise Exception(
                "No updated data available. Please run update_company_profile_data() first."
            )
            
        json_cols = [
            "shareholders",
            "directors",
            "commissioners",
            "audit_committees",
        ]

        date_now = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")

        if updated_rows_only:
            if self.updated_rows is None:
                print("No rows are updated. Your data is already up to date.")
            else:
                df = self.updated_rows.copy()
                filename = f"idx_company_profile_updated_rows_{date_now}.csv"

        else:
            df = self.new_data.copy()
            filename = f"idx_company_profile_all_rows_{date_now}.csv"
        
        df[json_cols] = df[json_cols].applymap(json.dumps)
        df.to_csv(filename, index=False)

    def upsert_to_db(self, save_current_data=True, supabase_client=None):
        """ Upsert updated data to idx_company_profile table in Supabase DB.
        """
        if not self.supabase_client:
            self.supabase_client = supabase_client
            
        if not self.supabase_client:
            raise Exception(
                "Can only upsert to DB if Supabase client is provided."
            )

        if self.new_data is None:
            print(
                "No updated data available. Please run update_company_profile_data() first if you haven't."
            )
            return
            
        def cast_int(num):
                if pd.notna(num):
                    return round(num)
                else:
                    return None
                
        def convert_df_to_records(df, int_cols=[]):
            temp_df = df.copy()
            for cols in temp_df.columns:
                if temp_df[cols].dtype == "datetime64[ns]":
                    temp_df[cols] = temp_df[cols].astype(str)
            temp_df = temp_df.replace({np.nan: None})
            records = temp_df.to_dict("records")
            
            for r in records:
                for k, v in r.items():
                    if k in int_cols:
                        r[k] = cast_int(v)
                        
            return records
        
        df = self.updated_rows.copy()
        print(df)
        logging.info(f"Upserting {df['symbol'].values} rows to idx_company_profile table.")
        df[["yf_currency", "wsj_format", "current_source"]] = df[["yf_currency", "wsj_format", "current_source"]].fillna(-1)
        df['nologo'] = df['nologo'].fillna(True)
        records = convert_df_to_records(df, int_cols=["sub_sector_id", "yf_currency", "wsj_format", "current_source"])
        self.supabase_client.table("idx_company_profile_duplicate").upsert(
            records, returning="minimal", on_conflict="symbol"
        ).execute()
        
        if save_current_data:
            self.current_data.to_csv('idx_company_profile_current.csv', index=False)


if __name__ == "__main__":
    LOG_FILENAME = 'scrapper.log'
    initiate_logging(LOG_FILENAME)
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--all_symbols', dest='all_symbols', type=bool, help='Check all symbols if this args enabled, otherwise only check new symbols. This args is set to False by default.')
    args = parser.parse_args()

    load_dotenv()
    url, key = os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY')
    proxy = os.getenv("proxy")
    # print(proxy)
    supabase_client = create_client(url, key)
    updater = IdxProfileUpdater(
        # company_profile_csv_path="company_profile.csv",
        supabase_client=supabase_client,
        proxy = proxy
    )
    if args.all_symbols:
        logging.info("Starting idx_profile_updater with all symbols")
        updater.update_company_profile_data(update_new_symbols_only=False)
    else:
        logging.info("Starting idx_profile_updater with new symbols and deactivated symbols")
        updater.update_company_profile_data(update_new_symbols_only=True)
    updater.upsert_to_db()
    logging.info("idx_profile_updater finished")

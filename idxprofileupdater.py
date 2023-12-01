import json

import numpy as np
import pandas as pd
import yfinance as yf
import ast
from bs4 import BeautifulSoup
from pyrate_limiter import Duration, Limiter, RequestRate
from requests import Session
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

def _convert_json_col_to_df(df, col_name):
    if df.empty:
        return None
    temp_df = df.loc[df[col_name].notna(),['symbol', col_name]].set_index('symbol')
    temp_df[col_name] = temp_df[col_name].apply(str)
    try:
        temp_df[col_name] = temp_df[col_name].apply(ast.literal_eval)
    except ValueError as e:
        pass
    temp_df = temp_df.explode(col_name)
    temp_df = temp_df[col_name].apply(pd.Series, dtype='object')
    temp_df = temp_df.dropna(axis=1, how='all')
    temp_df = temp_df.reset_index()
    temp_df.columns = temp_df.columns.str.lower()
    return temp_df

class LimiterSession(LimiterMixin, Session):
    def __init__(self):
        super().__init__(
            limiter=Limiter(
                RequestRate(2, Duration.SECOND * 5)
            ),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
        )

class OwnershipCleaner:
    def __init__(self, shareholders_df) -> None:
        self.current_shareholders_data = _convert_json_col_to_df(shareholders_df, 'shareholders')[['symbol','name','share_percentage']]
        self.current_shareholders_data['name_lower'] = self.current_shareholders_data['name'].str.lower()
        self.current_shareholders_data = self.current_shareholders_data.drop('name', axis=1)

    def _process_management_col_to_df(self, df, col_name):
        temp_df = _convert_json_col_to_df(df, col_name)
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
        
    def _process_shareholder_col_to_df(self, df, col_name, new_symbols=[]):
        shareholders_df = _convert_json_col_to_df(df, col_name)
        shareholders_df = shareholders_df.rename(columns={"summary": "share_amount", "percentage":"share_percentage"})
        shareholders_df = shareholders_df.drop_duplicates()
        
        shareholders_df[['share_amount', 'share_percentage']] = shareholders_df[['share_amount', 'share_percentage']].astype(str)
        shareholders_df['share_percentage'] = shareholders_df['share_percentage'].apply(lambda x: round(float(x.replace('%',''))/100,4))
        shareholders_df['share_amount'] = shareholders_df['share_amount'].apply(lambda x: float(x.replace(',','')))
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
        shareholders_df = shareholders_df.loc[shareholders_df['share_amount']>0]
        
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
        merged_df = merged_df.groupby(['symbol', 'name_lower', 'type']).agg({'name':'first', 'share_amount':'sum', 'share_percentage':'sum'}).reset_index()
        
        merged_df = pd.merge(merged_df,self.current_shareholders_data,
                                    how='left',on=['symbol','name_lower'],suffixes=['_new','_old'])
        merged_df['share_percentage_change'] = (merged_df['share_percentage_new'] - merged_df['share_percentage_old'])/merged_df['share_percentage_old']
        
        filter = (~merged_df['symbol'].isin(new_symbols) & merged_df['share_percentage_change'].isna())
        merged_df.loc[filter,'share_percentage_change'] = merged_df.loc[filter,'share_percentage_change'].fillna(merged_df['share_percentage_new'])
        merged_df['share_percentage_change'] = merged_df['share_percentage_change'].fillna(0)
        merged_df = merged_df.drop(columns=['share_percentage_old'])
        merged_df = merged_df.rename(columns={'share_percentage_new':'share_percentage'})
        merged_df['share_percentage_change'] = merged_df['share_percentage_change'].apply(lambda x: round(x,4))
        merged_df = merged_df.drop(columns=['name_lower'])
        
        return merged_df
    
    def process_ownership_col(self, df, col_name, new_symbols):
        if col_name in ['directors', 'commissioners', 'audit_committees']:
            temp_df = self._process_management_col_to_df(df, col_name)
        elif col_name == 'shareholders':
            temp_df = self._process_shareholder_col_to_df(df, col_name, new_symbols)

        temp_df = temp_df.replace(np.nan, None)
        json_df = temp_df.groupby('symbol').apply(lambda x: x.drop(columns=['symbol']).to_json(orient='records')).reset_index(name=col_name)
        json_df[col_name] = json_df.apply(lambda x: json.loads(x[col_name]), axis=1)
    
        return json_df

class IdxProfileUpdater:
    def __init__(
        self,
        company_profile_csv_path=None,
        supabase_client=None,
        chrome_driver_path="./chromedriver.exe",
    ):
        columns = [
            "company_name",
            "symbol",
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
            "employee_num",
            "yf_currency",
            "holders_breakdown",
        ]

        if company_profile_csv_path and supabase_client:
            raise ValueError(
                "Only one of company_profile_csv_path or supabase_client should be provided."
            )

        elif company_profile_csv_path:
            self.current_data = pd.read_csv(company_profile_csv_path)
            self.current_data = self.current_data[columns]

        elif supabase_client:
            response = (
                supabase_client.table("idx_company_profile").select("*").execute()
            )
            self.supabase_client = supabase_client
            self.current_data = pd.DataFrame(response.data)
            self.current_data = self.current_data[columns]

        else:
            self.current_data = pd.DataFrame(columns=columns)

        self.updated_data = None
        self.updated_rows = []
        self.modified_symbols = []
        self.ownershipcleaner = OwnershipCleaner(self.current_data[['symbol','shareholders']])
        self.chrome_driver_path = chrome_driver_path
        self._session = LimiterSession()

    def _retrieve_active_symbols(self):
        wd = webdriver.Chrome(service=Service(self.chrome_driver_path))
        url = "https://www.idx.co.id/en/market-data/stocks-data/stock-list/"
        wd.get(url)

        wait = WebDriverWait(wd, 20)
        select_element = wait.until(
            EC.visibility_of_element_located((By.NAME, "perPageSelect"))
        )
        select = Select(select_element)
        select.select_by_value("-1")
        bs = BeautifulSoup(wd.page_source, "lxml")
        wd.quit()

        active_symbols = []
        table = bs.find("table", id="vgt-table")
        tbody = table.find("tbody")
        for row in tbody.find_all("tr"):
            symbol = row.find("td").text.strip()
            active_symbols.append(symbol + ".JK")

        return active_symbols

    def _retrieve_profile_from_idx(self, yf_symbol):
        def extract_table_data(section_title):
            h4 = bs.find("h4", string=section_title)
            table = h4.find_next_sibling("table")
            headers = table.find("thead").findChildren("tr")
            rows = table.find("tbody").findChildren("tr")

            header_names = []
            for header in headers:
                header_cells = header.findChildren("th")
                for header_cell in header_cells:
                    header_names.append(header_cell.text.strip())

            data_list = []
            for row in rows:
                cell = row.findChildren("td")
                data = {
                    header_names[n]: cell[n].text.strip()
                    for n in range(len(header_names))
                }
                data_list.append(data)

            return data_list

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

        symbol = yf_symbol.split(".")[0]
        wd = webdriver.Chrome(service=Service(self.chrome_driver_path))
        url = f"https://www.idx.co.id/en/listed-companies/company-profiles/{symbol}"
        wd.get(url)
        wait = WebDriverWait(wd, 20)
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.bzg")))
        bs = BeautifulSoup(wd.page_source, "lxml")
        wd.quit()

        profile_dict = {"symbol": yf_symbol}

        profile_div = bs.find("div", class_="bzg")
        td_names = profile_div.find_all("td", class_="td-name")
        td_contents = profile_div.find_all("td", class_="td-content")
        key_renaming = {
            "Office Address": "address",
            "Register": "register",
            "Industry": "industry",
            "Sub-industry": "sub_industry",
            "Email Address": "email",
            "Fax": "fax",
            "Name": "company_name",
            "Listing Board": "listing_board",
            "Listing Date": "listing_date",
            "Phone": "phone",
            "Website": "website",
            "NPWP": "NPWP",
        }

        for td_name, td_content in zip(td_names, td_contents):
            key = td_name.text.strip()
            if key == "Subsector":
                value = td_content.text.strip()
                profile_dict["sub_sector_id"] = sub_sector_id_map.get(value, None)
            elif key in key_renaming.keys():
                renamed_key = key_renaming.get(key, key)
                value = td_content.text.strip()
                profile_dict[renamed_key] = value

        key_title_dict = {
            "shareholders": "Shareholders",
            "directors": "Director",
            "commissioners": "Comissioners",
            "audit_committees": "Audit Committee",
        }

        for key, title in key_title_dict.items():
            try:
                profile_dict[key] = extract_table_data(title)
            except:
                print(f"{title} data not available for {symbol} on IDX site.")

        profile_dict["delisting_date"] = None

        return profile_dict

    def _retrieve_data_from_yf_api(self, yf_symbol):
        ticker = yf.Ticker(yf_symbol, session=self._session)
        data_dict = {}
        try:
            ticker_info = ticker.info
        except:
            print(f"Ticker info not available for {yf_symbol} on YF API.")
            
        data_dict['employee_num'] = ticker_info.get('fullTimeEmployees')
        yf_currency_map = {'IDR':1,'USD':2}
        yf_currency = ticker_info.get('financialCurrency')
        data_dict['yf_currency'] = yf_currency_map.get(yf_currency)
        
        try:
            holders_breakdown = ticker.major_holders
        except:
            print(f"Holders breakdown data not available for {yf_symbol} on YF API.")
        
        if holders_breakdown.empty:
            data_dict["holders_breakdown"] = (
                    holders_breakdown.set_index(1)
                    .replace(np.nan, None)
                    .T.to_dict(orient="records")[0]
                )
        else:
            data_dict["holders_breakdown"] = None

        return data_dict

    def update_company_profile_data(
        self, update_new_symbols_only=True, data_to_update="all"
    ):
        """Update company profile data.

        Args:
            update_new_symbols_only (bool, optional): Whether to update only rows with new symbols or all rows. Defaults to True.
            data_to_update (str, optional): Which data to update: "profile_idx", "data_yf", or "all". Defaults to "all".
        """

        def update_profile_for_row(row):
            if data_to_update in ["profile_idx", "all"]:
                try:
                    profile_dict = self._retrieve_profile_from_idx(row["symbol"])
                    for key in profile_dict.keys():
                        company_profile_data.at[row.name, key] = profile_dict[key]
                except Exception as e:
                    print(
                        f"Failed to retrieve company profile for {row['symbol']} from IDX site. Error: {e}"
                    )
            if data_to_update in ["data_yf", "all"]:
                try:
                    yf_data_dict = self._retrieve_data_from_yf_api(row["symbol"])
                    for key in yf_data_dict.keys():
                        company_profile_data.at[row.name, key] = yf_data_dict[key]
                except Exception as e:
                    print(
                        f"Failed to retrieve additional data for {row['symbol']} from YF API. Error: {e}"
                    )
                    
        ### Management & Shareholders Cleaning
        def clean_ownership(df, columns, new_symbols):
            if data_to_update == 'data_yf':
                print('No ownership cleaning needed for updating data from YF')
                return df
            profile_df = df.copy()
            merged_updated_df = pd.DataFrame()
            try:
                for col_name in columns:
                    temp_df = self.ownershipcleaner.process_ownership_col(profile_df, col_name, new_symbols)
                    if merged_updated_df.empty:
                        merged_updated_df = temp_df.copy()
                    else:
                        merged_updated_df = pd.merge(merged_updated_df, temp_df, on="symbol", how="outer")
                # print(merged_updated_df.columns)      
                merged_updated_df = merged_updated_df.set_index('symbol')
                # check length of merged_updated_df and profile_df is same. Check only for cleaned columns (if columns are null, no need to include it)
                if len(profile_df.dropna(subset=columns_to_clean, how='all')) == len(merged_updated_df):  
                    profile_df = profile_df.set_index('symbol')
                    profile_df.update(merged_updated_df)
                    profile_df = profile_df.reset_index()
                else:
                    raise AssertionError("Error: Number of rows do not match") 
                return profile_df
                
            except Exception as e:
                print(f'Failed to clean shareholders columns, dropping uncleaned columns for upsert. Error: {e}')
                return None
                
        active_symbols = self._retrieve_active_symbols()
        company_profile_data = self.current_data.copy()
        csv_active_symbols = company_profile_data.query("delisting_date.isnull()")[
            "symbol"
        ].unique()
        updated_inactive_symbols = list(set(csv_active_symbols) - set(active_symbols))
        updated_new_symbols = list(set(active_symbols) - set(csv_active_symbols))

        inactive_filter = company_profile_data.query(
            "symbol in @updated_inactive_symbols"
        ).index
        company_profile_data.loc[
            inactive_filter, "delisting_date"
        ] = pd.Timestamp.now().strftime("%Y-%m-%d")
        self.modified_symbols.extend(updated_inactive_symbols)

        company_profile_data = pd.concat(
            [company_profile_data, pd.DataFrame({"symbol": updated_new_symbols})],
            ignore_index=True,
        )

        if update_new_symbols_only:
            new_rows_filter = company_profile_data.query(
                "symbol in @updated_new_symbols"
            ).index
            rows_to_update = company_profile_data.loc[new_rows_filter]
            self.modified_symbols.extend(updated_new_symbols)

        else:
            rows_to_update = company_profile_data
            self.modified_symbols.extend(active_symbols)

        rows_to_update.apply(update_profile_for_row, axis=1)
        columns_to_clean = [
                "shareholders",
                "directors",
                "commissioners",
                "audit_committees",
            ]
        updated_rows = clean_ownership(rows_to_update, columns_to_clean, updated_new_symbols)
        if updated_rows is not None:
            self.updated_data = updated_rows
            self.updated_rows = self.updated_data.query("symbol in @self.modified_symbols")
        else:
            rename_columns = []
            for col in columns_to_clean:
                if col in updated_rows.columns:
                    updated_rows[f'{col}_clean'] = updated_rows[col]
                    rename_columns.append(f'{col}_clean')
        
            self.updated_data = updated_rows
            self.updated_rows = self.updated_data.drop(
                rename_columns + columns_to_clean, axis=1
                ).query("symbol in @self.modified_symbols")

   
    def save_update_to_csv(self, updated_rows_only=True):
        """Generate CSV file containing updated data.

        Args:
            updated_rows_only (bool, optional): Whether to save only updated rows or all rows. Defaults to True.
        """
        if self.updated_data is None:
            raise Exception(
                "No updated data available. Please run update_company_profile_data() first."
            )
            
        json_cols = [
            "shareholders",
            "directors",
            "commissioners",
            "audit_committees",
            "holders_breakdown"
        ]

        date_now = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")

        if updated_rows_only:
            if self.updated_rows is None:
                print("No rows are updated. Your data is already up to date.")
            else:
                df = self.updated_rows.copy()
                filename = f"idx_company_profile_updated_rows_{date_now}.csv"

        else:
            df = self.updated_data.copy()
            filename = f"idx_company_profile_all_rows_{date_now}.csv"
        
        df[json_cols] = df[json_cols].applymap(json.dumps)
        df.to_csv(filename, index=False)

    def upsert_to_db(self):
        if self.supabase_client is None:
            raise Exception(
                "Can only upsert to DB if the class is initialized with Supabase client."
            )

        if self.updated_data is None:
            raise Exception(
                "No updated data available. Please run update_company_profile_data() first."
            )
            
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
            temp_df["updated_on"] = pd.Timestamp.now(tz="GMT").strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            temp_df = temp_df.replace({np.nan: None})
            records = temp_df.to_dict("records")
            
            for r in records:
                for k, v in r.items():
                    if k in int_cols:
                        r[k] = cast_int(v)
                        
            return records

        df = self.updated_rows.copy()
        records = convert_df_to_records(df, int_cols=["employee_num", "sub_sector_id", "yf_currency"])
        self.supabase_client.table("idx_company_profile").upsert(
            records, returning="minimal", on_conflict="symbol"
        ).execute()


if __name__ == "__main__":
    updater = IdxProfileUpdater(
        # company_profile_csv_path="company_profile.csv",
        chrome_driver_path='E:\Downloads\chromedriver-win64\chromedriver.exe'
    )
    updater.update_company_profile_data(
        update_new_symbols_only=False, data_to_update="all"
    )
    updater.save_update_to_csv(updated_rows_only=False)

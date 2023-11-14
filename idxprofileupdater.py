import argparse
import json
import re
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from pyrate_limiter import Duration, Limiter, RequestRate
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    def __init__(self):
        super().__init__(
            limiter=Limiter(
                RequestRate(2, Duration.SECOND * 5)
            ),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
            backend=SQLiteCache("yfinance.cache"),
        )


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
        self.chrome_driver_path = chrome_driver_path
        self._session = CachedLimiterSession()

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
            data_dict["employee_num"] = ticker.info["fullTimeEmployees"]
        except:
            print(f"Employee number data not available for {yf_symbol} on YF API.")
        try:
            data_dict["holders_breakdown"] = ticker.major_holders.set_index(
                1
            ).replace(np.nan, None).T.to_dict(orient="records")[0]
        except:
            print(f"Holders breakdown data not available for {yf_symbol} on YF API.")

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

        active_symbols = self._retrieve_active_symbols()
        company_profile_data = self.current_data.copy()
        csv_active_symbols = company_profile_data.query("delisting_date.isnull()")["symbol"].unique()
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
            new_rows_filter = company_profile_data.query("symbol in @updated_new_symbols").index
            rows_to_update = company_profile_data.loc[new_rows_filter]
            self.modified_symbols.extend(updated_new_symbols)

        else:
            rows_to_update = company_profile_data
            self.modified_symbols.extend(active_symbols)

        rows_to_update.apply(update_profile_for_row, axis=1)

        self.updated_data = company_profile_data
        self.updated_rows = self.updated_data.query("symbol in @self.modified_symbols")

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
            "holders_breakdown",
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
            raise Exception("Can only upsert to DB if the class is initialized with Supabase client.")
        
        if self.updated_data is None:
            raise Exception(
                "No updated data available. Please run update_company_profile_data() first."
            )

        def convert_df_to_records(df):
            temp_df = df.copy()
            for cols in temp_df.columns:
                if temp_df[cols].dtype == "datetime64[ns]":
                    temp_df[cols] = temp_df[cols].astype(str)
            temp_df["updated_on"] = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
            temp_df = temp_df.replace({np.nan: None})
            records = temp_df.to_dict("records")
            return records
        
        df = self.updated_rows.copy()
        df['sub_sector_id'] = df['sub_sector_id'].astype(int)
        records = convert_df_to_records(df)
        self.supabase_client.table("idx_company_profile").upsert(records, returning="minimal", on_conflict="symbol").execute()
        
        


if __name__ == "__main__":
    updater = IdxProfileUpdater(
        company_profile_csv_path="idx_company_profile_051023.csv"
    )
    updater.update_company_profile_data(
        update_new_symbols_only=True, data_to_update="all"
    )
    updater.save_update_to_csv(updated_rows_only=True)

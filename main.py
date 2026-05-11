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
import time
import json
import yfinance as yf
import translators as ts
import argparse
from fuzzywuzzy import fuzz
import logging
import re
from datetime import date
from fuzzywuzzy import process

# from imp import reload
from importlib import reload

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
    "alias",
    "subsidiaries",
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

    formatLOG = "%(asctime)s - %(levelname)s: %(message)s"
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO, format=formatLOG)
    logging.info("Program started")


def normalize_company_case(company_name: str) -> str:
    needs_cleaning = False

    upper_count = sum(1 for char in company_name if char.isupper())
    lower_count = sum(1 for char in company_name if char.islower())

    # Check if the string is mostly uppercase
    if upper_count > lower_count:
        needs_cleaning = True

    # Check if all words are capitalized
    words = company_name.split()
    if not needs_cleaning and not all(word[0].isupper() for word in words if word):
        needs_cleaning = True

    # Check if last letter of the last word capitalized
    if not needs_cleaning and words:
        last_word = words[-1]
        if last_word and last_word[-1].isalpha() and last_word[-1].isupper():
            needs_cleaning = True

    if needs_cleaning:
        cleaned_name = company_name.title()
        cleaned_name = re.sub(r"\bPt\.?\b", "PT", cleaned_name)
        return cleaned_name.strip()
    else:
        return company_name


def normalize_company_format(company_name: str) -> str:
    company_clean = re.sub(r"Tbk\.+", "Tbk", company_name, flags=re.IGNORECASE)
    company_clean = re.sub(
        r"\bTbk\b(?=.*\bTbk\b)", "", company_clean, flags=re.IGNORECASE
    )
    company_clean = re.sub(r"\s+", " ", company_clean).strip()

    return company_clean.strip()


class ProxyRequester:
    def __init__(self, proxy=None):
        """Initializes the ProxyRequester class with the provided proxy

        Args:
            proxy (str, optional): the proxy to be used. Defaults to None. Example: 'brd-customer-xxx-zone-xxx:xxx@brd.superproxy.io:xxx'
        """
        # Set up SSL context to unverified
        ssl._create_default_https_context = ssl._create_unverified_context
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

        if proxy:
            proxy_support = urllib.request.ProxyHandler({"http": proxy, "https": proxy})
            opener = urllib.request.build_opener(proxy_support)
            urllib.request.install_opener(opener)
        else:
            # Install an opener without proxy support
            opener = urllib.request.build_opener()
            urllib.request.install_opener(opener)

    def fetch_url(self, url):
        # Use the installed opener to fetch the URL
        try:
            print(f"Fetching: {url}")
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": self.user_agent,
                    "Referer": "https://www.idx.co.id/en-us/listed-companies/company-profiles",
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Connection": "keep-alive",
                },
            )
            with urllib.request.urlopen(req) as response:
                content = response.read().decode()
                print(f"Success! Response length: {len(content)}")
                return content
        except Exception as e:
            print(f"Error fetching URL {url}: {e}")
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
        self._ticker_maps_cache = None

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

        temp_df = df.loc[df[col_name].notna(), ["symbol", col_name]].set_index("symbol")

        try:
            temp_df[col_name] = temp_df[col_name].apply(json.loads)
        except TypeError as e:
            pass

        temp_df = temp_df.explode(col_name)
        temp_df = temp_df.dropna(subset=[col_name])
        temp_df = temp_df[col_name].apply(pd.Series, dtype="object")

        temp_df = temp_df.reset_index()
        temp_df.columns = temp_df.columns.str.lower()
        temp_df = temp_df.dropna(axis=1, how="all")
        return temp_df

    def _standardize_name_for_matching(self, name: str) -> str:
        if not isinstance(name, str):
            return ""
        name = name.lower()
        name = re.sub(r"\b(pt|tbk|persero|cv)\b", "", name)
        # Remove all punctuation and symbols
        name = re.sub(r"[^\w\s]", " ", name)
        words = sorted(name.split())
        return " ".join(words).strip()

    def _get_ticker_maps(self, supabase_client) -> dict | dict:
        if self._ticker_maps_cache:
            return self._ticker_maps_cache

        company_lists = (
            supabase_client.table("idx_company_profile")
            .select("symbol, company_name")
            .execute()
            .data
        )

        standardized_name_map = {}
        reverse_ticker_map = {}

        for company in company_lists:
            company_name = company.get("company_name")
            company_symbol = company.get("symbol")
            if company_name:
                cleaned_name = self._standardize_name_for_matching(company_name)
                standardized_name_map[cleaned_name] = company_symbol
                reverse_ticker_map[company_name] = company_symbol

        self._ticker_maps_cache = (standardized_name_map, reverse_ticker_map)
        return self._ticker_maps_cache

    def _process_management_col_to_df(self, df, col_name):
        """Processes the management column (directors, commissioners, or audit_committees) in the dataframe to a new dataframe

        Args:
            df (pd.DataFrame): the original dataframe
            col_name (str): the name of the management column

        Returns:
            pd.DataFrame: the processed dataframe
        """
        temp_df = self._convert_json_col_to_df(df, col_name)
        temp_df = temp_df.dropna(subset=["name", "position"])
        temp_df["position"] = temp_df["position"].str.title()
        temp_df["name"] = temp_df["name"].str.title()

        position_renaming_dicts = {
            "directors": {
                "Vice President": "Vice President Director",
                "Vice Presiden Director": "Vice President Director",
                "Presiden Direktur": "President Director",
                "Wakil Presiden Direktur": "Vice President Director",
                "Direktur": "Director",
                "Direktur Utama": "President Director",
                "Wakil Direktur Utama": "Vice President Director",
                "": "Director",
            },
            "commissioners": {
                "President Commisioner": "President Commissioner",
                "Vice President Commisioner": "Vice President Commissioner",
                "Presiden Komisaris": "President Commissioner",
                "Komisaris": "Commissioner",
                "Wakil Komisaris Utama": "Vice President Commissioner",
                "Komisaris Utama": "President Commissioner",
                "Wakil Presiden Komisaris": "Vice President Commissioner",
                "": "Commissioner",
            },
            "audit_committees": {
                "Ketua": "Head of Audit Committee",
                "Anggota": "Member of Audit Committee",
                "Ketua Komite Audit": "Head of Audit Committee",
                "Anggota Komite Audit": "Member of Audit Committee",
                "Head": "Head of Audit Committee",
                "Member": "Member of Audit Committee",
                "": "Audit Committee",
            },
        }

        temp_df["position"] = temp_df["position"].replace(
            position_renaming_dicts[col_name]
        )
        temp_df = temp_df.drop_duplicates(subset=["symbol", "name", "position"])

        return temp_df

    def _process_shareholder_col_to_df(self, df, col_name, supabase_client):
        """Processes the shareholder column in the dataframe to a new dataframe

        Args:
            df (pd.DataFrame): the original dataframe
            col_name (str): the name of the shareholder column

        Returns:
            pd.DataFrame: the processed dataframe
        """

        def convert_share_percentage(x):
            try:
                return round(float(x.replace("%", "")) / 100, 8)
            except Exception as e:
                print(f"Error: {e}. Value is {x}")
                return None

        def convert_share_amount(x):
            try:
                return float(x.replace(",", ""))
            except Exception as e:
                print(f"Error: {e}. Value is {x}")
                return None

        shareholders_df = self._convert_json_col_to_df(df, col_name)
        shareholders_df = shareholders_df.drop_duplicates()

        old_cols = ["share_amount", "share_percentage"]
        new_cols = ["share_amount_new", "share_percentage_new"]

        for cols in old_cols:
            if cols not in shareholders_df.columns:
                shareholders_df[cols] = None

        old_shareholders_df = shareholders_df[
            shareholders_df["share_amount"].notna()
        ].copy()
        old_shareholders_df = old_shareholders_df.drop(
            columns=new_cols, errors="ignore"
        )

        shareholders_df = shareholders_df.drop(index=old_shareholders_df.index)
        shareholders_df = shareholders_df.drop(columns=old_cols, errors="ignore")

        # Ensure columns exist before processing
        for col in new_cols:
            if col not in shareholders_df.columns:
                shareholders_df[col] = "0"

        shareholders_df[["share_amount_new", "share_percentage_new"]] = shareholders_df[
            ["share_amount_new", "share_percentage_new"]
        ].astype(str)
        # shareholders_df['share_percentage_new'] = shareholders_df['share_percentage_new'].apply(lambda x: round(float(x.replace('%',''))/100,4))
        shareholders_df["share_percentage_new"] = shareholders_df[
            "share_percentage_new"
        ].apply(convert_share_percentage)
        # shareholders_df['share_amount_new'] = shareholders_df['share_amount_new'].apply(lambda x: float(x.replace(',','')))
        shareholders_df["share_amount_new"] = shareholders_df["share_amount_new"].apply(
            convert_share_amount
        )

        # Fixing share_amount_new where it's 0 but share_percentage_new > 0
        count_amount_fixed = 0
        count_percentage_fixed = 0

        for symbol in shareholders_df["symbol"].unique():
            symbol_df = shareholders_df[shareholders_df["symbol"] == symbol]

            valid_references = symbol_df[
                (symbol_df["share_amount_new"] > 0)
                & (symbol_df["share_percentage_new"] > 0)
            ]
            reference_row = valid_references.loc[
                valid_references["share_percentage_new"].idxmax()
            ]

            share_value = (
                reference_row["share_amount_new"]
                / reference_row["share_percentage_new"]
            )

            rows_amount_to_fix = symbol_df[
                (symbol_df["share_amount_new"] == 0)
                & (symbol_df["share_percentage_new"] > 0)
            ]
            if not rows_amount_to_fix.empty:
                count_amount_fixed += 1
                for index, row in rows_amount_to_fix.iterrows():
                    calculated_amount = share_value * row["share_percentage_new"]
                    shareholders_df.loc[index, "share_amount_new"] = calculated_amount

            rows_percentage_to_fix = symbol_df[
                (symbol_df["share_percentage_new"] == 0)
                & (symbol_df["share_amount_new"] > 0)
            ]
            if not rows_percentage_to_fix.empty:
                count_percentage_fixed += 1
                for index, row in rows_percentage_to_fix.iterrows():
                    calculated_percentage = row["share_amount_new"] / share_value
                    shareholders_df.loc[index, "share_percentage_new"] = (
                        calculated_percentage
                    )

        logging.info(
            f"Fixed {count_amount_fixed} total symbols with 0 share_amount but >0 share_percentage"
        )
        logging.info(
            f"Fixed {count_percentage_fixed} total symbols with 0 share_percentage but >0 share_amount"
        )

        shareholders_df.loc[shareholders_df["name"] == "Saham Treasury", "type"] = (
            "Treasury Stock"
        )

        name_mapping = {
            "Saham Treasury": "Treasury Stock",
            "Pengendali Saham": "Controlling Shareholder",
            "Non Pengendali Saham": "Non Controlling Shareholder",
            "Masyarakat Warkat": "Public (Scrip)",
            "Masyarakat Non Warkat": "Public (Scripless)",
            "Masyarakat": "Public",
            "MASYARAKAT": "Public",
            "Publik": "Public",
            "PUBLIK": "Public",
            "Masyarakat Lainnya": "Other Public",
            "Negara Republik Indonesia": "Republic of Indonesia",
            "NEGARA REPUBLIK INDONESIA": "Republic of Indonesia",
            "Kejaksaan Agung": "Attorney General",
            "KEJAKSAAN AGUNG": "Attorney General",
            "Direksi": "Director",
            "AFILIASI PENGENDALI": "Controlling Affiliate",
            "Pihak Afiliasi ": "Affiliate Parties",
            "Pihak Afilasi": "Affiliate Parties",
            "0": np.nan,
            "-": np.nan,
            "": np.nan,
        }
        shareholders_df = shareholders_df.replace({"name": name_mapping})

        shareholders_df = shareholders_df.loc[
            (shareholders_df["share_amount_new"] > 0)
            & (shareholders_df["share_percentage_new"] > 0)
        ]
        shareholders_df = shareholders_df.loc[
            shareholders_df["share_percentage_new"] > 0.00001
        ]

        type_mapping = {
            "Direksi": "Director",
            "Commisioner": "Commissioner",
            "Komisaris": "Commissioner",
            "Kurang dari 5%": "Less Than 5%",
            "Lebih dari 5%": "More Than 5%",
            "Saham Pengendali": "Controlling Share",
            "Saham Non Pengendali": "Non Controlling Share",
            "Masyarakat Warkat": "Scrip Public Share",
            "Masyarakat Non Warkat": "Scripless Public Share",
            "": "-",
        }

        shareholders_df = shareholders_df.replace({"type": type_mapping})

        shareholders_df["name"] = shareholders_df["name"].str.title()

        directors_df = self._process_management_col_to_df(df, "directors")
        directors_df = directors_df.drop_duplicates(subset=["symbol", "name"])
        directors_df = directors_df.query("name != '-'")
        directors_df["name_lower"] = directors_df["name"].str.lower()

        commissioners_df = self._process_management_col_to_df(df, "commissioners")
        commissioners_df = commissioners_df.drop_duplicates(subset=["symbol", "name"])
        commissioners_df = commissioners_df.query("name != '-'")
        commissioners_df["name_lower"] = commissioners_df["name"].str.lower()

        shareholders_df["name_lower"] = shareholders_df["name"].str.lower()
        merged_df = pd.merge(
            shareholders_df,
            directors_df[["symbol", "name_lower", "position"]],
            left_on=["symbol", "name_lower"],
            right_on=["symbol", "name_lower"],
            how="left",
        )
        merged_df = pd.merge(
            merged_df,
            commissioners_df[["symbol", "name_lower", "position"]],
            left_on=["symbol", "name_lower"],
            right_on=["symbol", "name_lower"],
            how="left",
            suffixes=["_dir", "_comm"],
        )

        merged_df["type"] = np.where(
            merged_df["position_dir"].notna(),
            merged_df["position_dir"],
            merged_df["type"],
        )
        merged_df["type"] = np.where(
            merged_df["position_comm"].notna() & merged_df["position_dir"].isna(),
            merged_df["position_comm"],
            merged_df["type"],
        )

        # Symbol identification for shareholders
        if supabase_client:
            standardized_map, reverse_map = self._get_ticker_maps(supabase_client)
            company_name_choices = list(reverse_map.keys())

            merged_df["ticker"] = None

            for index, row in merged_df.iterrows():
                shareholder_name = row["name"]

                cleaned_shareholder_key = self._standardize_name_for_matching(
                    shareholder_name
                )
                found_ticker = standardized_map.get(cleaned_shareholder_key)
                print(
                    f"Matching {shareholder_name} with cleaned key {cleaned_shareholder_key} to ticker {found_ticker}"
                )

                if found_ticker:
                    merged_df.loc[index, "ticker"] = found_ticker

                if not found_ticker and "tbk" in shareholder_name.lower():
                    best_match = process.extractOne(
                        shareholder_name, company_name_choices
                    )
                    print(f"best match fuzzy: {best_match}")
                    if best_match and best_match[1] >= 90:
                        matched_name = best_match[0]
                        found_ticker = reverse_map[matched_name]
                        merged_df.loc[index, "ticker"] = found_ticker

        # Normalize name case and format
        company_mask = merged_df["name"].str.lower().str.contains("pt", na=False)
        merged_df.loc[company_mask, "name"] = merged_df.loc[company_mask, "name"].apply(
            normalize_company_case
        )
        merged_df.loc[company_mask, "name"] = merged_df.loc[company_mask, "name"].apply(
            normalize_company_format
        )

        merged_df = (
            merged_df.groupby(["symbol", "name_lower", "type"])
            .agg(
                {
                    "name": "first",
                    "share_amount_new": "sum",
                    "share_percentage_new": "sum",
                    "ticker": "first",
                }
            )
            .reset_index()
        )

        merged_df = merged_df.drop(columns=["name_lower"])
        merged_df = merged_df.rename(
            columns={
                "share_amount_new": "share_amount",
                "share_percentage_new": "share_percentage",
            }
        )
        merged_df = pd.concat([merged_df, old_shareholders_df], ignore_index=True)

        return merged_df

    def _process_subsidiary_col_to_df(self, df, col_name, supabase_client):
        """Processes the subsidiary column in the dataframe to a new dataframe

        Args:
            df (pd.DataFrame): the original dataframe
            col_name (str): the name of the subsidiary column
            supabase_client (Client): Supabase client object

        Returns:
            pd.DataFrame: the processed dataframe
        """
        subs_df = self._convert_json_col_to_df(df, col_name)
        if subs_df is None or subs_df.empty:
            return subs_df

        subs_df["ticker"] = None

        if supabase_client:
            standardized_map, reverse_map = self._get_ticker_maps(supabase_client)
            company_name_choices = list(reverse_map.keys())

            for index, row in subs_df.iterrows():
                sub_name = row.get("name")
                if not sub_name or not isinstance(sub_name, str):
                    continue

                cleaned_sub_key = self._standardize_name_for_matching(sub_name)
                found_ticker = standardized_map.get(cleaned_sub_key)

                if not found_ticker:
                    # User requested fuzzy search
                    best_match = process.extractOne(sub_name, company_name_choices)
                    if best_match and best_match[1] >= 90:
                        found_ticker = reverse_map[best_match[0]]

                if found_ticker:
                    subs_df.loc[index, "ticker"] = found_ticker

                if not found_ticker and "tbk" in sub_name.lower():
                    best_match = process.extractOne(sub_name, company_name_choices)
                    if best_match and best_match[1] >= 90:
                        matched_name = best_match[0]
                        found_ticker = reverse_map[matched_name]
                        subs_df.loc[index, "ticker"] = found_ticker

        # Process assets and units for all rows, regardless of supabase_client
        for index, row in subs_df.iterrows():
            try:
                # Handle both '.' and ',' as separators.
                # In ID format, '.' is thousands and ',' is decimal.
                # However, IDX sometimes provides values like '8.578' which could be 8578.
                raw_assets_str = str(row.get("total_assets", "0")).strip()

                # If there's both a dot and a comma, it's likely standard ID format: 1.234,56
                if "." in raw_assets_str and "," in raw_assets_str:
                    raw_assets_str = raw_assets_str.replace(".", "").replace(",", ".")
                # If there's only a dot, and it looks like a thousand separator (e.g. 3 digits after)
                elif "." in raw_assets_str and len(raw_assets_str.split(".")[-1]) == 3:
                    raw_assets_str = raw_assets_str.replace(".", "")
                # If there's only a comma, it's likely a decimal separator
                elif "," in raw_assets_str:
                    raw_assets_str = raw_assets_str.replace(",", ".")

                raw_assets = float(raw_assets_str)
                unit = str(row.get("unit", "")).lower()

                # Unit conversion logic
                unit_multipliers = {
                    "thousands": 1000,
                    "millions": 1000000,
                    "billions": 1000000000,
                    "trillions": 1000000000000,
                    "ribuan": 1000,
                    "jutaan": 1000000,
                    "miliaran": 1000000000,
                    "triliunan": 1000000000000,
                    "full": 1,
                }
                multiplier = unit_multipliers.get(unit, 1)
                subs_df.loc[index, "total_assets"] = raw_assets * multiplier
            except Exception as e:
                # print(f"Error processing assets for {row.get('name')}: {e}")
                subs_df.loc[index, "total_assets"] = 0

        # Drop unit column as it's been incorporated into total_assets
        if "unit" in subs_df.columns:
            subs_df = subs_df.drop(columns=["unit"])

        return subs_df

    def process_ownership_col(
        self, df: pd.DataFrame, col_name: str, supabase_client=None
    ) -> pd.DataFrame:
        """
        Process the ownership column (directors, commissioners, audit_committees or shareholders) in a dataframe to a json format

        Args:
            df (pd.DataFrame): dataframe to be processed
            col_name (str): column name to be processed
            supabase_client (Client, optional): Supabase client object for database interactions. Defaults to None.
        Returns:
            pd.DataFrame: processed dataframe containing the ownership column in json format
        """
        if col_name in [
            "directors",
            "commissioners",
            "audit_committees",
            "subsidiaries",
        ]:
            if col_name == "subsidiaries":
                temp_df = self._process_subsidiary_col_to_df(
                    df, col_name, supabase_client
                )
            else:
                temp_df = self._process_management_col_to_df(df, col_name)
        elif col_name == "shareholders":
            temp_df = self._process_shareholder_col_to_df(df, col_name, supabase_client)

        temp_df = temp_df.replace(np.nan, None)
        json_df = (
            temp_df.groupby("symbol")
            .apply(
                lambda x: x.rename(
                    columns={"ticker": "symbol"}, errors="ignore"
                ).to_json(orient="records"),
                include_groups=False,
            )
            .reset_index(name=col_name)
        )
        json_df[col_name] = json_df.apply(lambda x: json.loads(x[col_name]), axis=1)

        return json_df


class IdxProfileUpdater:
    def __init__(self, company_profile_csv_path=None, supabase_client=None, proxy=None):
        """
        Class to update idx_company_profile table in supabase database.

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
            self.current_data = pd.read_csv(
                company_profile_csv_path, usecols=all_columns
            )

        elif supabase_client:
            response = (
                supabase_client.table("idx_company_profile").select("*").execute()
            )
            self.supabase_client = supabase_client
            self.current_data = pd.DataFrame(response.data, columns=all_columns)
            if not self.current_data.empty:
                self.current_data = self.current_data.drop_duplicates(
                    subset="symbol", keep="last"
                )

        else:
            self.supabase_client = None
            self.current_data = pd.DataFrame(columns=all_columns)

        self.new_data = None
        self.updated_rows = None
        self.modified_symbols = set()
        self.ownershipcleaner = OwnershipCleaner()
        self._session = LimiterSession()
        self._requester = ProxyRequester(proxy)
        self._translation_cache = {}

    def _retrieve_active_symbols(self):
        url = "https://www.idx.co.id/primary/StockData/GetSecuritiesStock?start=0&length=9999&code=&sector=&board=&language=en-us"
        response = self._requester.fetch_url(url)
        if response == False:
            raise Exception("Error retrieving active symbols from IDX json.")

        data = json.loads(response)["data"]
        active_symbols = {index["Code"] + ".JK": index["Name"] for index in data}
        print(len(active_symbols), "active symbols")

        return active_symbols

    def _retrieve_new_ipo_symbols(self, supabase_client=None):
        if not self.supabase_client:
            self.supabase_client = supabase_client

        if not self.supabase_client:
            raise Exception("Can only upsert to DB if Supabase client is provided.")

        today = date.today().strftime("%Y-%m-%d")
        new_tickers = (
            self.supabase_client.table("idx_ipo_details")
            .select("symbol")
            .lte("listing_date", today)
            .execute()
        )
        new_symbols = [item["symbol"] for item in new_tickers.data]

        return new_symbols

    @limits(calls=2, period=4)
    def _retrieve_idx_profile(self, yf_symbol):
        symbol = (yf_symbol.split(".")[0]).lower()
        url = f"https://www.idx.co.id/primary/ListedCompany/GetCompanyProfilesDetail?KodeEmiten={symbol}&language=en-us"
        response = self._requester.fetch_url(url)
        if response == False:
            raise Exception(f"Failed to fetch profile for {yf_symbol} from IDX.")

        data = json.loads(response)

        profile_dict = {"symbol": yf_symbol}

        if data.get("Profiles") and len(data["Profiles"]) > 0:
            profiles = data["Profiles"][0]
        else:
            profiles = {}
            print(
                f"WARNING: Profile data empty for {yf_symbol}. Using available details."
            )

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
            "Nama": "name",
            "Jabatan": "position",
            "Afiliasi": "affiliated",
            "Independen": "independent",
            "Jumlah": "share_amount_new",
            "Kategori": "type",
            "Persentase": "share_percentage_new",
        }

        subsidiaries_renaming = {
            "Nama": "name",
            "BidangUsaha": "business_activity",
            "JumlahAset": "total_assets",
            "Lokasi": "location",
            "MataUang": "currency",
            "Persentase": "percentage",
            "Satuan": "unit",
            "StatusOperasi": "operation_status",
            "TahunKomersil": "commercial_year",
        }

        status_mapping = {
            "beroperasi": "Operating",
            "aktif": "Operating",
            "ya": "Operating",
            "sudah beroperasi": "Operating",
            "belum beroperasi": "Not Yet Operating",
            "belum belum beroperasi": "Not Yet Operating",
            "tidak beroperasi": "Non-Operating",
            "tahap pengembangan": "Development Stage",
            "pra-operasi": "Pre-Operating",
        }

        unit_mapping = {
            "jutaan": "Millions",
            "ribuan": "Thousands",
            "satuan": "Units",
            "penuh": "Full",
        }

        truth_dict = {False: "No", True: "Yes"}

        for key, value in profiles.items():
            if key.lower() == "subsektor":
                profile_dict["sub_sector_id"] = sub_sector_id_map.get(value, None)

            elif key in key_renaming.keys():
                renamed_key = key_renaming.get(key, key)
                raw_value = str(value).strip()

                if renamed_key == "company_name":
                    clean_company_case = normalize_company_case(raw_value)
                    clean_company = normalize_company_format(clean_company_case)
                    profile_dict[renamed_key] = clean_company
                else:
                    profile_dict[renamed_key] = raw_value

        def _change_bool_to_string(list_dict, key_name):
            for i in range(len(list_dict)):
                list_dict[i][key_name] = truth_dict.get(list_dict[i][key_name])
            return list_dict

        def _clean_dict(list_dict, key_name=None):
            if not list_dict:
                return None
            if key_name:
                list_dict = _change_bool_to_string(list_dict, key_name)
            for dct in list_dict.copy():
                for key in list(dct.keys()):
                    dct[shareholders_renaming.get(key, key)] = dct.pop(key)
            return list_dict

        directors = data["Direktur"]
        directors = _clean_dict(directors, "Afiliasi")
        profile_dict["directors"] = directors

        commissioners = data["Komisaris"]
        commissioners = _clean_dict(commissioners, "Independen")
        profile_dict["commissioners"] = commissioners

        audit_committees = data["KomiteAudit"]
        audit_committees = _clean_dict(audit_committees)
        profile_dict["audit_committees"] = audit_committees

        shareholders_data = data.get("PemegangSaham", [])
        shareholders = [
            {
                key: str(value).strip()
                for key, value in sub.items()
                if key != "Pengendali"
            }
            for sub in shareholders_data
        ]
        shareholders = _clean_dict(shareholders)
        profile_dict["shareholders"] = shareholders

        subsidiaries_raw = data.get("AnakPerusahaan", [])
        if subsidiaries_raw:
            print(
                f"DEBUG: Found {len(subsidiaries_raw)} entries in AnakPerusahaan for {yf_symbol}"
            )
        else:
            print(f"DEBUG: No AnakPerusahaan found for {yf_symbol}")

        subsidiaries = []
        for sub in subsidiaries_raw:
            renamed_sub = {}
            for key, value in sub.items():
                new_key = subsidiaries_renaming.get(key, key)
                new_value = value

                if new_key == "operation_status" and isinstance(value, str):
                    new_value = status_mapping.get(value.lower().strip(), value)
                elif new_key == "unit" and isinstance(value, str):
                    new_value = unit_mapping.get(value.lower().strip(), value)
                elif new_key == "commercial_year":
                    if value == "0" or value == 0:
                        new_value = ""
                elif new_key == "business_activity" and value:
                    val_str = str(value).strip()
                    if val_str in self._translation_cache:
                        new_value = self._translation_cache[val_str]
                    else:
                        # Retry logic for translation
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                # Use translate_text from translators library
                                new_value = ts.translate_text(
                                    val_str,
                                    from_language="id",
                                    to_language="en",
                                    translator="google",
                                )
                                self._translation_cache[val_str] = new_value
                                break
                            except Exception as e:
                                if attempt < max_retries - 1:
                                    wait_time = (attempt + 1) * 2  # 2s, 4s
                                    print(
                                        f"Translation failed for '{val_str}', retrying in {wait_time}s... ({e})"
                                    )
                                    time.sleep(wait_time)
                                else:
                                    print(
                                        f"Translation failed after {max_retries} attempts for '{val_str}': {e}"
                                    )
                                    new_value = val_str  # Fallback to original

                if isinstance(new_value, str):
                    # Clean up all whitespace including \r\n
                    new_value = " ".join(new_value.split())

                renamed_sub[new_key] = new_value
            subsidiaries.append(renamed_sub)

        profile_dict["subsidiaries"] = subsidiaries if subsidiaries else None

        profile_dict["delisting_date"] = None
        return profile_dict

    def update_company_profile_data(
        self, update_new_symbols_only=True, target_symbols=None, limit=None
    ):
        """Update company profile data.

        Args:
            update_new_symbols_only (bool, optional): Whether to update only rows with new symbols or all rows. Defaults to True.
            limit (int, optional): Limit the number of symbols to update.
        """

        def update_profile_for_row(row):
            temp_row = row.copy()
            use_selenium = False
            try:
                profile_dict = self._retrieve_idx_profile(row["symbol"])
                for key in profile_dict.keys():
                    temp_row[key] = profile_dict[key]
                print("new data", profile_dict)
            except Exception as e:
                print(f"Failed to update profile for {row['symbol']}: {e}")
                return row  # Return original row if update fails
            time.sleep(3)

            # replace '-','0','' with None
            replace_cols = [
                "address",
                "email",
                "phone",
                "fax",
                "NPWP",
                "website",
                "register",
            ]
            temp_row[replace_cols] = temp_row[replace_cols].replace(
                ["-", "0", ""], [None, None, None]
            )

            temp_row["updated_on"] = pd.Timestamp.now(tz="GMT").strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            if (
                not pd.isna(row["company_name"])
                and temp_row["company_name"] != row["company_name"]
            ):
                print("isna")
                print(f"Company name updated for {temp_row['symbol']}.")
                print(f"Old name: {row['company_name']}")
                temp_row["alias"] = row["alias"].append(row["company_name"])

            if pd.isna(row["company_name"]):
                temp_row["alias"] = []

            return temp_row

        ### Management & Shareholders Cleaning
        def clean_ownership(df, columns):
            profile_df = df.copy()
            merged_updated_df = pd.DataFrame()

            for col_name in columns:
                temp_df = self.ownershipcleaner.process_ownership_col(
                    profile_df, col_name, self.supabase_client
                )
                if merged_updated_df.empty:
                    merged_updated_df = temp_df.copy()
                else:
                    merged_updated_df = pd.merge(
                        merged_updated_df, temp_df, on="symbol", how="outer"
                    )
            return merged_updated_df

        retrieved_active_company = {}
        retrieved_active_symbols = []
        try:
            retrieved_active_company = self._retrieve_active_symbols()
            retrieved_active_symbols = [symbol for symbol in retrieved_active_company]
            print(f"Retrieved {len(retrieved_active_symbols)} active symbols")

            # SAFEGUARD: Prevent mass delisting if API fails or returns suspiciously low symbols
            if len(retrieved_active_symbols) < 800:
                raise Exception(
                    f"Suspicously low number of active symbols retrieved ({len(retrieved_active_symbols)}). IDX might be blocking us or API changed. Aborting delisting check to prevent data corruption."
                )

        except Exception as e:
            print(f"Error fetching active symbols: {e}")
            return

        company_profile_data = self.current_data.copy()
        # Ensure no duplicates at the start
        company_profile_data = company_profile_data.drop_duplicates(
            subset="symbol", keep="last"
        )
        table_active_symbols = company_profile_data.query("delisting_date.isnull()")[
            "symbol"
        ].unique()
        new_ipo_symbols = self._retrieve_new_ipo_symbols(self.supabase_client)

        updated_inactive_symbols = list(
            set(table_active_symbols) - set(retrieved_active_symbols)
        )

        updated_new_symbols = list(
            (set(new_ipo_symbols) | set(retrieved_active_symbols))
            - set(table_active_symbols)
        )

        updated_inactive_filter = company_profile_data.query(
            "symbol in @updated_inactive_symbols"
        ).index
        company_profile_data.loc[updated_inactive_filter, "delisting_date"] = (
            pd.Timestamp.now().strftime("%Y-%m-%d")
        )
        self.modified_symbols.update(updated_inactive_symbols)

        company_profile_data = pd.concat(
            [company_profile_data, pd.DataFrame({"symbol": updated_new_symbols})],
            ignore_index=True,
        )

        if target_symbols:
            missing_target_symbols = list(
                set(target_symbols) - set(company_profile_data["symbol"])
            )
            if missing_target_symbols:
                company_profile_data = pd.concat(
                    [
                        company_profile_data,
                        pd.DataFrame({"symbol": missing_target_symbols}),
                    ],
                    ignore_index=True,
                )

        if update_new_symbols_only:
            if not target_symbols:
                target_symbols = updated_new_symbols

            updated_company_name_symbols = []

            with open("bypass-symbols.json") as bypass_file:
                bypass_symbols = json.load(bypass_file).get("symbols", [])
            for _, row in company_profile_data.iterrows():
                if row["symbol"] in bypass_symbols:
                    continue
                try:
                    similarity = fuzz.ratio(
                        row["company_name"][0:30].lower(),
                        retrieved_active_company[row["symbol"]].lower(),
                    )
                    if similarity <= 70:
                        updated_company_name_symbols.append(row["symbol"])
                except Exception as e:
                    pass

            print("Possible updated company name: ", updated_company_name_symbols)

            updated_new_filter = company_profile_data.query(
                "symbol in @updated_new_symbols and symbol in @target_symbols"
            ).index

            updated_new_filter = updated_new_filter.union(
                company_profile_data.query(
                    "symbol in @updated_company_name_symbols and symbol in @target_symbols"
                ).index
            )

            rows_to_update = company_profile_data.loc[updated_new_filter].copy()

        else:
            if target_symbols:
                active_filter = company_profile_data.query(
                    "symbol in @target_symbols"
                ).index
            else:
                active_filter = company_profile_data.query(
                    "symbol in @retrieved_active_symbols"
                ).index
            rows_to_update = company_profile_data.loc[active_filter].copy()

        if limit:
            rows_to_update = rows_to_update.head(limit)

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
            "subsidiaries",
        ]

        # Columns to drop for DB upsert if cleaning fails (only those that are uncleaned)
        # but we KEEP subsidiaries for the CSV output
        cols_to_drop_for_db = [
            c
            for c in columns_to_clean
            if c in rows_to_update.columns and c != "subsidiaries"
        ]

        try:
            cleaned_rows = clean_ownership(rows_to_update, columns_to_clean)
        except Exception as e:
            print(f"Failed to clean ownership columns: {e}")
            # Map existing json columns to string for the uncleaned CSV if needed
            # but we keep them as-is for the main saving logic
            existing_cols = [c for c in columns_to_clean if c in rows_to_update.columns]
            if existing_cols:
                # We save a copy with dumped strings for the uncleaned log
                temp_rows = rows_to_update[["symbol"] + existing_cols].copy()
                temp_rows[existing_cols] = temp_rows[existing_cols].map(
                    lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                )
                temp_rows.to_csv("ownership_data_uncleaned.csv", index=False)
        else:
            # Successfully cleaned
            rows_to_update.set_index("symbol", inplace=True)
            cleaned_rows_indexed = cleaned_rows.set_index("symbol")
            rows_to_update.update(cleaned_rows_indexed)
            rows_to_update.reset_index(inplace=True)

        # Update company_profile_data safely
        company_profile_data = company_profile_data.set_index("symbol")
        rows_to_update_indexed = rows_to_update.set_index("symbol")
        # Ensure object columns are handled correctly to avoid float64 warnings
        # and make sure new columns like 'subsidiaries' are added if not present
        for col in rows_to_update_indexed.columns:
            if col not in company_profile_data.columns:
                company_profile_data[col] = None
            company_profile_data[col] = company_profile_data[col].astype(object)

        company_profile_data.update(rows_to_update_indexed)
        company_profile_data = company_profile_data.reset_index()

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
            "subsidiaries",
        ]

        date_now = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")

        if updated_rows_only:
            if self.updated_rows is None:
                print("No rows are updated. Your data is already up to date.")
                return
            else:
                df = self.updated_rows.copy()
                filename = f"idx_company_profile_updated_rows_{date_now}.csv"

        else:
            df = self.new_data.copy()
            filename = f"idx_company_profile_all_rows_{date_now}.csv"

        # Apply JSON dump to all list/dict columns, ignoring nulls
        for col in json_cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                )

        df.to_csv(filename, index=False)

    def upsert_to_db(self, save_current_data=True, supabase_client=None):
        """Upsert updated data to idx_company_profile table in Supabase DB."""
        if not self.supabase_client:
            self.supabase_client = supabase_client

        if not self.supabase_client:
            raise Exception("Can only upsert to DB if Supabase client is provided.")

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

            for record in records:
                for k, v in record.items():
                    if k in int_cols:
                        record[k] = cast_int(v)

                    if "shareholders" in record and record["shareholders"] is not None:
                        final_shareholders = []
                        for shareholder in record["shareholders"]:
                            if "ticker" in shareholder:
                                shareholder["symbol"] = shareholder.pop("ticker")

                            if (
                                "symbol" in shareholder
                                and shareholder.get("symbol") is None
                            ):
                                del shareholder["symbol"]

                            if (
                                "share_percentage" in shareholder
                                and shareholder["share_percentage"] is not None
                            ):
                                share_percentage = shareholder.get("share_percentage")
                                if share_percentage is not None:
                                    share_percentage_str = str(share_percentage)
                                    if (
                                        "e" in share_percentage_str
                                        or "E" in share_percentage_str
                                    ):
                                        shareholder["share_percentage"] = (
                                            f"{share_percentage:.8f}".rstrip("0")
                                        )

                            final_shareholders.append(shareholder)

                record["shareholders"] = final_shareholders
            return records

        df = self.updated_rows.copy()

        # Print specifically symbol, delisting_date, and subsidiaries for verification
        print("\n" + "=" * 80)
        print("PREVIEW: DATA TO BE UPSERTED TO SUPABASE")
        print("=" * 80)
        for _, row in df.iterrows():
            print(f"SYMBOL: {row.get('symbol')}")
            print(f"DELISTING DATE: {row.get('delisting_date')}")
            subs = row.get("subsidiaries")
            print("SUBSIDIARIES:")
            if isinstance(subs, (list, dict)):
                print(json.dumps(subs, indent=2))
            else:
                print(subs)
            print("-" * 40)
        print("=" * 80 + "\n")
        logging.info(
            f"Upserting {df['symbol'].values} rows to idx_company_profile table."
        )
        df[["yf_currency", "wsj_format", "current_source"]] = (
            df[["yf_currency", "wsj_format", "current_source"]]
            .fillna(-1)
            .infer_objects(copy=False)
        )
        df["nologo"] = df["nologo"].fillna(True).infer_objects(copy=False)
        records = convert_df_to_records(
            df,
            int_cols=["sub_sector_id", "yf_currency", "wsj_format", "current_source"],
        )
        # print(f"Check records: {records}")

        self.supabase_client.table("idx_company_profile").upsert(
            records, returning="minimal", on_conflict="symbol"
        ).execute()

        if save_current_data:
            self.current_data.to_csv("idx_company_profile_current.csv", index=False)


if __name__ == "__main__":
    LOG_FILENAME = "scrapper.log"
    initiate_logging(LOG_FILENAME)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--all_symbols",
        dest="all_symbols",
        type=bool,
        help="Check all symbols if this args enabled, otherwise only check new symbols. This args is set to False by default.",
    )
    parser.add_argument(
        "--limit",
        dest="limit",
        type=int,
        default=None,
        help="Limit the number of symbols to check.",
    )
    parser.add_argument(
        "--symbols",
        dest="symbols",
        type=str,
        default=None,
        help="Target specific symbols (comma-separated).",
    )
    args = parser.parse_args()

    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    proxy = os.getenv("PROXY_URL") or os.getenv("proxy")
    # print(proxy)
    supabase_client = create_client(url, key)
    updater = IdxProfileUpdater(
        # company_profile_csv_path="company_profile.csv",
        supabase_client=supabase_client,
        proxy=proxy,
    )
    target_symbols = None
    if args.symbols:
        target_symbols = [s.strip() for s in args.symbols.split(",")]

    if args.all_symbols:
        logging.info("Starting idx_profile_updater with all symbols")
        updater.update_company_profile_data(
            update_new_symbols_only=False,
            limit=args.limit,
            target_symbols=target_symbols,
        )
    else:
        logging.info(
            "Starting idx_profile_updater with new symbols and deactivated symbols"
        )
        updater.update_company_profile_data(
            update_new_symbols_only=True,
            limit=args.limit,
            target_symbols=target_symbols,
        )

    updater.upsert_to_db()
    logging.info("idx_profile_updater finished")

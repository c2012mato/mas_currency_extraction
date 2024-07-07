import pandas as pd
import json
import requests
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime

class Configs:
    BIGQUERY_PROJECT_ID = "bigquery-production-PROJECT"
    BIGQUERY_DATASET = "DATASET"
    DESTINATION_TABLE = "TABLE"
    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform"
    ]

class GoogleCloudProcess:
    def __init__(self, configs):
        self.configs = configs
        self.client = None
        self.latest_end_of_day = None
        self.credentials = self.get_credentials()
    
    def get_credentials(self):
        json_string = {
            "type": "service_account",
            "project_id": "PROJECT",
            "private_key_id": "PKEY_ID",
            "private_key": "PKEY",
            "client_email": "SERVICE ACCOUNT EMAIL",
            "client_id": "CLIENT_ID",
            "auth_uri": "AUTH",
            "token_uri": "TOKEN_URI",
            "auth_provider_x509_cert_url": "AUTH_CERT",
            "client_x509_cert_url": "CLIENT_CERT",
        }

        credentials = service_account.Credentials.from_service_account_info(
            json_string,
            scopes=self.configs.SCOPES,
        )
        return credentials
    
    def connect_to_bigquery(self):
        credentials = self.credentials
        self.client = bigquery.Client(
            credentials=credentials,
            project=self.configs.BIGQUERY_PROJECT_ID,
            location="asia-southeast1",
        )
    
    def get_latest_end_of_day(self):
        table_id = f"{self.configs.BIGQUERY_PROJECT_ID}.{self.configs.BIGQUERY_DATASET}.{self.configs.DESTINATION_TABLE}"
        try:
            query = f"""
                SELECT max(end_of_day)
                FROM `{table_id}`
            """
            job = self.client.query(query)
            results = job.result()
            for row in results:
                self.latest_end_of_day = row[0]
            print(f"Latest end_of_day found: `{self.latest_end_of_day}`")
        except Exception as e:
            self.latest_end_of_day = datetime(2000, 1, 1)
            print(f"Error: {e}. Set default to `{self.latest_end_of_day}`")
    
    def upload_to_bigquery(self, df):
        credentials = self.credentials
        table_ref = f"{self.configs.BIGQUERY_PROJECT_ID}.{self.configs.BIGQUERY_DATASET}.{self.configs.DESTINATION_TABLE}"
        client = self.get_client_bigquery()
        
        df_final = df_final[df_final["end_of_day"] > self.latest_end_of_day]
        if len(df_final) > 0:
            print(f"Writing {len(df_final):,} rows to BigQuery...")
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.PARQUET,
            )
            job = client.load_table_from_dataframe(
                df,
                table_ref,
                job_config=job_config,
            )
            job.result()
            print(f"Data uploaded to {table_ref}")
            print('Finished!')
        else:
            print("Nothing to write!")
            print('Finished!')

class DataProcessor:
    def __init__(self, configs):
        self.configs = configs
        self.session = requests.session()
        self.client = None
        self.latest_end_of_day = None
        self.cloud_processor = GoogleCloudProcess(self.configs)
    
    def fetch_data_from_api(self):
        url = "https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=95932927-c8bc-4e7a-b484-68a66a24edfe"
        response = self.session.get(url)
        df_api = pd.DataFrame(response.json()["result"]["records"])
        df_api["end_of_day"] = pd.to_datetime(df_api["end_of_day"])
        df_api["timestamp"] = df_api["timestamp"].astype("int")
        list_fx_cols = [col for col in df_api.columns if col not in ["end_of_day", "timestamp"] and "_" in col]
        df_api[list_fx_cols] = df_api[list_fx_cols].astype("float")
        return df_api
    
    def fill_missing_dates(self, df_api):
        df_full_dates = pd.DataFrame(pd.date_range(df_api["end_of_day"].min(), df_api["end_of_day"].max(), freq='d'), columns=["end_of_day"])
        df = pd.merge(df_full_dates, df_api, on="end_of_day", how="left")
        df = df.sort_values(by="end_of_day")
        ffil_cols = [col for col in df.columns if col != "end_of_day"]
        df[ffil_cols] = df[ffil_cols].ffill()
        return df
    
    def adjust_fx_rates(self, df):
        new_list_fx_cols = []
        for col in [col for col in df.columns if "_" in col]:
            list_values = col.split("_")
            if len(list_values) == 3:
                divisor = int(list_values[2])
                df[col] = df[col] / divisor

            from_currency = list_values[0]
            to_currency = list_values[1]

            if from_currency == "usd":
                df[from_currency] = 1.0
                new_list_fx_cols.append(from_currency)

                df[to_currency] = 1 / df[col]
                new_list_fx_cols.append(to_currency)
            else:
                df[from_currency] = df[col] / df["usd_sgd"]
                new_list_fx_cols.append(from_currency)
        
        return df, new_list_fx_cols
    
    def prepare_final_dataframe(self, df, new_list_fx_cols):
        df_final = pd.DataFrame()
        for col in new_list_fx_cols:
            df_extract = df[["end_of_day", "timestamp", col]]
            df_extract = df_extract.rename(columns={col: "fx_rate"})
            df_extract["currency"] = col
            df_extract = df_extract[~df_extract["fx_rate"].isna()]
            df_extract = df_extract[["end_of_day", "timestamp", "currency", "fx_rate"]]
            df_final = pd.concat([df_final, df_extract], axis=0, ignore_index=True)
        return df_final
    
    def process_data(self):
        self.cloud_processor.connect_to_bigquery()
        self.cloud_processor.get_latest_end_of_day()

        df_api = self.fetch_data_from_api()
        df = self.fill_missing_dates(df_api)
        df, new_list_fx_cols = self.adjust_fx_rates(df)
        df_final = self.prepare_final_dataframe(df, new_list_fx_cols)
        
        self.cloud_processor.upload_to_bigquery(df_final)

def main ():
    configs = Configs()
    processor = DataProcessor(configs)
    processor.process_data()

#run
if __name__ == "__main__":
    main()
    

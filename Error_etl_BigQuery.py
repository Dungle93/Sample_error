from sqlalchemy import create_engine
import pandas as pd
from google.cloud import bigquery

def etl():
    '''
    This function works as follows:
    1. Connect Postgres
    2. Access Transaction table 
    3. convert currency
    4. push to Bigquery
    '''
    engine = create_engine("postgresql://user:pass@host:5432/dbname")
    df = pd.read_sql("SELECT * FROM transactions", engine)
    df['amount_usd'] = df['amount'] * 23000  # Convert VND to USD

    client = bigquery.Client()
    client.load_table_from_dataframe(df, "project.dataset.table")

etl()

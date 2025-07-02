from sqlalchemy import create_engine
import pandas as pd
from google.cloud import bigquery

def etl():
    engine = create_engine("postgresql://user:pass@host:5432/dbname")
    df = pd.read_sql("SELECT * FROM transactions", engine)
    df['amount_usd'] = df['amount'] * 23000  # Convert VND to USD

    client = bigquery.Client()
    client.load_table_from_dataframe(df, "project.dataset.table")

etl()

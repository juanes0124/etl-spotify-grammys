import pandas as pd
import psycopg2

def extract_spotify(filepath='/opt/airflow/data/spotify_dataset.csv'):
    """Extrae datos del CSV de Spotify"""
    df = pd.read_csv(filepath, index_col=0)
    print(f"Spotify extraído: {df.shape[0]} filas")
    return df

def extract_grammy(host='datawarehouse', port=5432,
                   database='music_dw', user='dwuser', password='dwpassword'):
    """Extrae datos de Grammy desde PostgreSQL"""
    conn = psycopg2.connect(
        host=host, port=port,
        database=database, user=user, password=password
    )
    df = pd.read_sql("SELECT * FROM grammy_raw;", conn)
    conn.close()
    print(f"Grammy extraído: {df.shape[0]} filas")
    return df
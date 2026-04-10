import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def load_to_warehouse(df, host='datawarehouse', port=5432,
                      database='music_dw', user='dwuser', password='dwpassword'):
    conn = psycopg2.connect(host=host, port=port,
                            database=database, user=user, password=password)
    cur = conn.cursor()

    # Eliminar tabla si existe y recrear
    cur.execute("DROP TABLE IF EXISTS music_fact;")
    cur.execute("""
        CREATE TABLE music_fact AS
        SELECT * FROM (SELECT 1 LIMIT 0) t;
    """)
    conn.commit()

    # Crear tabla basada en el dataframe
    col_defs = []
    for col, dtype in df.dtypes.items():
        if 'int' in str(dtype):
            col_defs.append(f'"{col}" BIGINT')
        elif 'float' in str(dtype):
            col_defs.append(f'"{col}" FLOAT')
        elif 'bool' in str(dtype):
            col_defs.append(f'"{col}" BOOLEAN')
        else:
            col_defs.append(f'"{col}" TEXT')

    cur.execute("DROP TABLE IF EXISTS music_fact;")
    cur.execute(f"CREATE TABLE music_fact ({', '.join(col_defs)});")
    conn.commit()

    # Insertar datos en lotes
    cols = [f'"{c}"' for c in df.columns.tolist()]
    df = df.where(pd.notnull(df), None)
    values = [tuple(row) for row in df.itertuples(index=False)]

    insert_query = f"INSERT INTO music_fact ({', '.join(cols)}) VALUES %s"
    execute_values(cur, insert_query, values, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Cargadas {len(df)} filas al Data Warehouse")

def load_to_csv(df, filepath='/opt/airflow/data/merged_dataset.csv'):
    df.to_csv(filepath, index=False)
    print(f"✅ CSV guardado en {filepath}")
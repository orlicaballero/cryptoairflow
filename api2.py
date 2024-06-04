import requests
import json
import psycopg2
import pandas as pd
from datetime import datetime
import pytz

def main_function():
    api_url = 'https://api.coincap.io/v2/assets'
    response = requests.get(api_url)
    data = json.loads(response.text)['data']

    crypto_df = pd.DataFrame(data)

    crypto_df = crypto_df[['id', 'name', 'symbol', 'priceUsd', 'marketCapUsd', 'volumeUsd24Hr', 'changePercent24Hr']]
    crypto_df.columns = ['id', 'nombre', 'simbolo', 'precio_usd', 'capitalizacion_usd', 'volumen_24h_usd', 'variacion_24h']

    crypto_df['precio_usd'] = pd.to_numeric(crypto_df['precio_usd'], errors='coerce')
    crypto_df['capitalizacion_usd'] = pd.to_numeric(crypto_df['capitalizacion_usd'], errors='coerce')
    crypto_df['volumen_24h_usd'] = pd.to_numeric(crypto_df['volumen_24h_usd'], errors='coerce')
    crypto_df['variacion_24h'] = pd.to_numeric(crypto_df['variacion_24h'], errors='coerce')

    crypto_df.dropna(inplace=True)
    crypto_df.drop_duplicates(subset=['id'], keep='last', inplace=True)

    local_timezone = pytz.timezone("America/Argentina/Buenos_Aires")
    local_time = datetime.now(local_timezone)

    url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    database = "data-engineer-database"
    user = "orlandocaballerook_coderhouse"
    pwd = "xX54X9OcGv"

    try:
        conn = psycopg2.connect(
            host=url,
            dbname=database,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Conexión a Redshift exitosa!")
    except Exception as e:
        print("No es posible conectar a Redshift.")
        print(e)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS criptomonedas (
                id VARCHAR(50),
                fecha_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                nombre VARCHAR(255),
                simbolo VARCHAR(10),
                precio_usd DECIMAL(18, 2),
                capitalizacion_usd DECIMAL(18, 2),
                volumen_24h_usd DECIMAL(18, 2),
                variacion_24h DECIMAL(5, 2),
                PRIMARY KEY (id, fecha_ingesta)
            );
        """)
        conn.commit()

    print("Tabla creada en Redshift.")

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE criptomonedas_temp (
                id VARCHAR(50),
                fecha_ingesta TIMESTAMP,
                nombre VARCHAR(255),
                simbolo VARCHAR(10),
                precio_usd DECIMAL(18, 2),
                capitalizacion_usd DECIMAL(18, 2),
                volumen_24h_usd DECIMAL(18, 2),
                variacion_24h DECIMAL(5, 2)
            );
        """)
        conn.commit()

    with conn.cursor() as cur:
        for index, row in crypto_df.iterrows():
            cur.execute("""
                INSERT INTO criptomonedas_temp (id, fecha_ingesta, nombre, simbolo, precio_usd, capitalizacion_usd, volumen_24h_usd, variacion_24h)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (row['id'], local_time, row['nombre'], row['simbolo'], row['precio_usd'], row['capitalizacion_usd'], row['volumen_24h_usd'], row['variacion_24h']))
        conn.commit()

    print("Datos cargados en la tabla temporal de Redshift.")

    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM criptomonedas
            USING criptomonedas_temp
            WHERE criptomonedas.id = criptomonedas_temp.id;

            INSERT INTO criptomonedas
            SELECT criptomonedas_temp.id, criptomonedas_temp.fecha_ingesta, criptomonedas_temp.nombre, criptomonedas_temp.simbolo, criptomonedas_temp.precio_usd, criptomonedas_temp.capitalizacion_usd, criptomonedas_temp.volumen_24h_usd, criptomonedas_temp.variacion_24h
            FROM criptomonedas_temp;
        """)
        conn.commit()

    print("Datos cargados y actualizados en la tabla principal de Redshift.")
    conn.close()

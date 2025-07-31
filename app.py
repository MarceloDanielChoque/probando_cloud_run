import os
import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import storage
from io import StringIO
from datetime import datetime
from flask import Flask

# Cargar variables de entorno
load_dotenv()

def main():
    try:
        credenciales = service_account.Credentials.from_service_account_file(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
    except Exception as e:
        print(f"Error auth google: {e}")
        return  # Evita continuar si hay error

    # Crear cliente de Google Sheets
    sheet_service = build('sheets', 'v4', credentials=credenciales)
    sheet_client = sheet_service.spreadsheets()
    
    # Leer datos de paises
    datos_paises = sheet_client.values().get(
        spreadsheetId=os.getenv('FILE_ID_PAISES'),
        range='paises!A1:Z1000'
    ).execute().get('values', [])

    df_paises = pd.DataFrame(data=datos_paises[1:], columns=datos_paises[0])
    dict_paises = dict(zip(df_paises.iloc[:, 0], df_paises.iloc[:, 1]))

    # Leer datos de ventas
    datos_ventas = sheet_client.values().get(
        spreadsheetId=os.getenv('FILE_ID_VENTAS'),
        range='ventas!A1:Z1000'
    ).execute().get('values', [])

    df_ventas = pd.DataFrame(data=datos_ventas[1:], columns=datos_ventas[0])
    df_ventas['Región'] = df_ventas['Región'].replace(dict_paises)

    # Leer datos de vendedores
    datos_vendedores = sheet_client.values().get(
        spreadsheetId=os.getenv('FILE_ID_VENDEDORES'),
        range='hoja_vendedores!A1:Z1000'
    ).execute().get('values', [])

    df_vendedores = pd.DataFrame(data=datos_vendedores[1:], columns=datos_vendedores[0])

    # Subir archivos a GCS
    subir_df_a_gcs(df_ventas, 'bucket_proyecto1_daniel_1', 'fichero_ventas')
    subir_df_a_gcs(df_paises, 'bucket_proyecto1_daniel_1', 'fichero_paises')
    subir_df_a_gcs(df_vendedores, 'bucket_proyecto1_daniel_1', 'fichero_vendedores')


def subir_df_a_gcs(df, nombre_bucket, nombre_base):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nombre_archivo = f"{nombre_base}_{timestamp}.csv"

    client = storage.Client()
    bucket = client.bucket(nombre_bucket)
    blob = bucket.blob(nombre_archivo)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"Archivo '{nombre_archivo}' subido a bucket '{nombre_bucket}' con éxito.")


# ---- Flask app para Cloud Run ----

app = Flask(__name__)

@app.route('/')
def run_job():
    main()
    return "Ejecución completada", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

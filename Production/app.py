import pandas as pd
from azure.storage.blob import BlobClient
from fastapi import APIRouter

def read_parquet_from_blob(blob_client):
    """
    Lee los datos del archivo Parquet desde Azure Blob Storage.

    Args:
        blob_client: El cliente de Blob Storage.

    Returns:
        Un DataFrame con los datos del archivo Parquet.
    """

    #Obtiene el nombre del archivo Parquet.
    filename=blob_client.blob_name

    #Lee el archivo Parquet.
    df=pd.read_parquet(filename)

    return df

def detect_anomalies(df):
    """
    Aplica la regla de detección de anomalías a los datos.

    Args:
        df: Un DataFrame con los datos.

    Returns:
        Un DataFrame con la columna 'alert' con valores 1 o 0 según las condiciones especificadas.
    """

    #Aplica la transformación de datos
    df['transaction_amount']=df['transaction_amount'].astype('float')
    df=df[df['transaction_type'] == 'DEBITO'].reset_index(drop=True)
    df['transaction_day']=df['transaction_date'].dt.date
    df_agrupado=df.groupby(['transaction_day', 'user_id', 'account_number'])['transaction_amount'].agg(['count', 'mean', 'std', 'sum']).reset_index()
    df_agrupado=df_agrupado.rename(columns={'count': 'num_transactions', 'mean': 'mean_transaction_amount', 'std': 'std_transaction_amount', 'sum': 'total_transaction_amount'})
    df_agrupado=df_agrupado.fillna(0)

    #Crea la nueva columna 'alert' con valores 1 o 0.
    df_agrupado['alert']=(df_agrupado['num_transactions'] >= 5) & (df_agrupado['std_transaction_amount'] <= 8.4)

    #Convierte los valores True/False en 1/0 en la columna 'alert'.
    df_agrupado['alert']=df_agrupado['alert'].astype(int)

    return df_agrupado

router = APIRouter()

@router.post("/anomalies")
async def detect_anomalies(file: bytes):
    """
    Devuelve los resultados de la detección de anomalías.

    Args:
        file: Los datos de entrada en formato Parquet.

    Returns:
        Un DataFrame con la columna 'alert' con valores 1 o 0 según las condiciones especificadas.
    """

    #Crea un cliente de Blob Storage.
    blob_client=BlobClient.from_blob_url(file.content)

    #Lee los datos del archivo Parquet.
    df=read_parquet_from_blob(blob_client)

    #Aplica la regla de detección de anomalías.
    df=detect_anomalies(df)

    return df

import os
from datetime import timedelta
import yfinance as yf
import pandas as pd

TICKERS = {"DJI": "^DJI", "NASDAQ": "^IXIC", "SP500": "^GSPC"}

RAW_DIR = "data/raws"
os.makedirs(RAW_DIR, exist_ok=True)

def descargar_indices(start_date, end_date, guardar_csv=True):
    datos = {}

    for nombre, ticker in TICKERS.items():
        df = yf.download(ticker, start=start_date, end=end_date)
        df = df[["Open", "Close"]]

        if guardar_csv:
            ruta_csv = os.path.join(RAW_DIR, f"{nombre}_historico.csv")
            df.to_csv(ruta_csv)

        datos[nombre] = df

    return datos

def cargar_indices_desde_csv():
    datos = {}

    for nombre in TICKERS.keys():
        ruta_csv = os.path.join(RAW_DIR, f"{nombre}_historico.csv")
        df = pd.read_csv(ruta_csv, parse_dates=["Date"], index_col="Date")
        datos[nombre] = df

    return datos

def obtener_valores_por_fecha(df, fecha, dias_adelante=7):
    fecha_obj = pd.to_datetime(fecha)

    if fecha_obj not in df.index:
        fecha_obj = df.index[df.index.get_loc(fecha_obj, method="ffill")]

    valor_hoy = df.loc[fecha_obj, "Close"]

    fecha_ventana = fecha_obj + timedelta(days=dias_adelante)

    if fecha_ventana not in df.index:
        fecha_ventana = df.index[df.index.get_loc(fecha_ventana, method="ffill")]
        
    valor_ventana = df.loc[fecha_ventana, "Close"]

    return valor_hoy, valor_ventana


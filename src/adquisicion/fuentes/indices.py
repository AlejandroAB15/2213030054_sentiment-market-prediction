import os
from datetime import timedelta
import yfinance as yf
import pandas as pd

TICKERS = {
    "DOW_JONES": "^DJI",
    "NASDAQ": "^IXIC",
    "SP500": "^GSPC"
}

RAW_DIR = "data/raws"
os.makedirs(RAW_DIR, exist_ok=True)


def descargar_indices(start_date, end_date, guardar_csv=True):
    datos = {}

    for nombre, ticker in TICKERS.items():
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            progress=False
        )

        df = df[["Open", "Close"]]
        df.reset_index(inplace=True)
        df.columns.name = None

        if guardar_csv:
            ruta_csv = os.path.join(RAW_DIR, f"{nombre}_historico.csv")
            df.to_csv(ruta_csv, index=False)

        df.set_index("Date", inplace=True)
        datos[nombre] = df

    return datos


def cargar_indices_desde_csv():
    datos = {}

    for nombre in TICKERS.keys():
        ruta_csv = os.path.join(RAW_DIR, f"{nombre}_historico.csv")

        df = pd.read_csv(
            ruta_csv,
            parse_dates=["Date"]
        )

        df.set_index("Date", inplace=True)
        datos[nombre] = df

    return datos


def obtener_valores_por_fecha(df, fecha_noticia, dias_adelante=7):
    fecha_obj = pd.to_datetime(
        fecha_noticia,
        dayfirst=True,
        errors="coerce"
    )

    if pd.isna(fecha_obj):
        return None, None

    if fecha_obj not in df.index:
        fecha_obj = df.index[df.index.get_loc(fecha_obj, method="ffill")]

    valor_hoy = df.loc[fecha_obj, "Close"]

    fecha_futura = fecha_obj + timedelta(days=dias_adelante)

    if fecha_futura not in df.index:
        fecha_futura = df.index[
            df.index.get_loc(fecha_futura, method="ffill")
        ]

    valor_futuro = df.loc[fecha_futura, "Close"]

    return float(valor_hoy), float(valor_futuro)

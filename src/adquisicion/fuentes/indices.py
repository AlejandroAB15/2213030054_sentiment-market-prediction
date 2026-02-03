import os
from datetime import timedelta
import yfinance as yf
import pandas as pd
from utils.logger import get_logger
from utils.logging_utils import log_and_print

logger = get_logger(__name__)

TICKERS = {
    "DJI": "^DJI",
    "NASDAQ": "^IXIC",
    "SP500": "^GSPC"
}

RAW_DIR = "data/raws"
os.makedirs(RAW_DIR, exist_ok=True)

def descargar_indices(start_date, end_date, guardar_csv=True):
    datos = {}

    log_and_print(
        logger,
        f"[INDICES] Descargando índices desde {start_date} hasta {end_date}"
    )

    for nombre, ticker in TICKERS.items():
        try:
            log_and_print(
                logger,
                f"[INDICES] Descargando {nombre} ({ticker})"
            )

            df = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                progress=False
            )

            if df.empty:
                log_and_print(
                    logger,
                    f"[INDICES] {nombre} no devolvió datos",
                    level="warning"
                )
                continue

            df = df[["Open", "Close"]]

            if guardar_csv:
                ruta_csv = os.path.join(
                    RAW_DIR,
                    f"{nombre}_historico.csv"
                )
                df.to_csv(ruta_csv)

                log_and_print(
                    logger,
                    f"[INDICES] {nombre} guardado en {ruta_csv}"
                )

            datos[nombre] = df

        except Exception as e:
            log_and_print(
                logger,
                f"[INDICES] Error descargando {nombre}: {e}",
                level="error"
            )

    log_and_print(
        logger,
        f"[INDICES] Descarga completada ({len(datos)} índices)"
    )

    return datos

def cargar_indices_desde_csv():
    datos = {}

    log_and_print(
        logger,
        "[INDICES] Cargando índices desde CSV"
    )

    for nombre in TICKERS.keys():
        ruta_csv = os.path.join(
            RAW_DIR,
            f"{nombre}_historico.csv"
        )

        if not os.path.exists(ruta_csv):
            log_and_print(
                logger,
                f"[INDICES] CSV no encontrado: {ruta_csv}",
                level="warning"
            )
            continue

        try:
            df = pd.read_csv(
                ruta_csv,
                parse_dates=["Date"],
                index_col="Date"
            )

            datos[nombre] = df

            log_and_print(
                logger,
                f"[INDICES] {nombre} cargado ({len(df)} filas)"
            )

        except Exception as e:
            log_and_print(
                logger,
                f"[INDICES] Error cargando {nombre}: {e}",
                level="error"
            )

    return datos

def obtener_valores_por_fecha(df, fecha, dias_adelante=7):
    try:
        fecha_obj = pd.to_datetime(fecha)

        if fecha_obj not in df.index:
            fecha_obj = df.index[
                df.index.get_loc(fecha_obj, method="ffill")
            ]

        valor_hoy = df.loc[fecha_obj, "Close"]

        fecha_ventana = fecha_obj + timedelta(days=dias_adelante)

        if fecha_ventana not in df.index:
            fecha_ventana = df.index[
                df.index.get_loc(fecha_ventana, method="ffill")
            ]

        valor_ventana = df.loc[fecha_ventana, "Close"]

        return valor_hoy, valor_ventana

    except Exception as e:
        log_and_print(
            logger,
            f"[INDICES] Error obteniendo valores para {fecha}: {e}",
            level="error"
        )
        return None, None

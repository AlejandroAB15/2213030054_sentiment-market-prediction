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
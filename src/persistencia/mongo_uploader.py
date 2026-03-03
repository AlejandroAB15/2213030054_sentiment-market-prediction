from datetime import datetime
from pathlib import Path
import json
import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from utils.logger import get_logger
from utils.logging_utils import log_and_print

logger = get_logger(__name__)

def _conectar():
    load_dotenv()

    uri_mongo = os.getenv("MONGO_URI")

    try:
        cliente = MongoClient(uri_mongo)
        return cliente
    except Exception as e:
        logger.error(f"Error al conectar con MongoDB: {e}")
        raise


def _convertir_dataframe(df: pd.DataFrame) -> list[dict]:
    documentos = []

    for indice, fila in df.iterrows():
        try:
            fecha = pd.to_datetime(fila["fecha"], errors="coerce")
            cierre_sp500_t = float(fila["close_sp500_t"])
            cierre_sp500_t7 = float(fila["close_sp500_t7"])
            cierre_nasdaq_t = float(fila["close_nasdaq_t"])
            cierre_nasdaq_t7 = float(fila["close_nasdaq_t7"])
            cierre_dji_t = float(fila["close_dji_t"])
            cierre_dji_t7 = float(fila["close_dji_t7"])

            documento = {
                "titulo": fila["titulo"],
                "subtitulo": fila["subtitulo"] if pd.notna(fila["subtitulo"]) else None,
                "fuente": fila["fuente"],
                "fecha": fecha.to_pydatetime(),
                "sentimiento": fila["sentimiento_label"],

                "close_sp500_t": cierre_sp500_t,
                "close_sp500_t7": cierre_sp500_t7,
                "variacion_sp500_7d": cierre_sp500_t7 - cierre_sp500_t,

                "close_nasdaq_t": cierre_nasdaq_t,
                "close_nasdaq_t7": cierre_nasdaq_t7,
                "variacion_nasdaq_7d": cierre_nasdaq_t7 - cierre_nasdaq_t,

                "close_dji_t": cierre_dji_t,
                "close_dji_t7": cierre_dji_t7,
                "variacion_dji_7d": cierre_dji_t7 - cierre_dji_t,

                "fecha_creacion": datetime.utcnow(),
            }

            documentos.append(documento)

        except Exception as e:
            logger.error(f"Error al transformar la fila {indice}: {e}")

    return documentos


def _obtener_estadisticas(
    ruta_relevantes: Path,
    ruta_no_relevantes: Path,
):
    try:
        with open(ruta_relevantes, "r", encoding="utf-8") as f:
            relevantes = json.load(f)

        with open(ruta_no_relevantes, "r", encoding="utf-8") as f:
            no_relevantes = json.load(f)

        total_relevantes = len(relevantes)
        total_no_relevantes = len(no_relevantes)
        total_original = total_relevantes + total_no_relevantes

        return total_original, total_relevantes, total_no_relevantes

    except Exception as e:
        logger.error(f"Error accediendo a json de preprocesado: {e}")
        raise


def upload_to_mongo(
    df_final: pd.DataFrame,
    ruta_relevantes: Path,
    ruta_no_relevantes: Path,
):
    cliente = None

    try:
        cliente = _conectar()
        base_datos = cliente["trabajo_terminal"]

        coleccion_articulos = base_datos["articulos"]
        coleccion_estadisticas = base_datos["estadisticas_dataset"]

        log_and_print(logger, "Iniciando transformación del DataFrame...")

        documentos = _convertir_dataframe(df_final)

        log_and_print(
            logger,
            f"Total de documentos preparados para insertar: {len(documentos)}"
        )

        log_and_print(logger, "Reemplazando colección 'articulos'...")

        coleccion_articulos.delete_many({})

        if documentos:
            coleccion_articulos.insert_many(documentos)

        log_and_print(logger, "Actualizando estadísticas generales del dataset...")

        total_original, total_relevantes, total_no_relevantes = _obtener_estadisticas(
            ruta_relevantes,
            ruta_no_relevantes,
        )

        documento_estadisticas = {
            "total_original": total_original,
            "total_relevantes": total_relevantes,
            "total_no_relevantes": total_no_relevantes,
            "fecha_actualizacion": datetime.utcnow(),
        }

        coleccion_estadisticas.replace_one({}, documento_estadisticas, upsert=True)

        log_and_print(logger, "Carga a MongoDB finalizada correctamente.")

    except Exception as e:
        logger.error(f"Error durante la carga a MongoDB: {e}")
        raise

    finally:
        if cliente:
            cliente.close()
import json
from pathlib import Path
from typing import Dict, List
import pandas as pd
from utils.logging_utils import log_and_print

def _leer_json(path: Path) -> pd.DataFrame:

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    return df

def unir_datasets(
    sources: Dict[str, str],
    logger
) -> pd.DataFrame:

    log_and_print(logger, "\n[PREPROCESADO] Inicio unión de datasets")

    dataframes: List[pd.DataFrame] = []

    for fuente, path in sources.items():

        log_and_print(logger, f"[PREPROCESADO] Cargando fuente: {fuente}")

        path_obj = Path(path)

        if not path_obj.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {path}")

        df = _leer_json(path_obj)

        df["fuente"] = fuente

        dataframes.append(df)

        log_and_print(logger,f"[PREPROCESADO] Registros cargados ({fuente}): {len(df)}")

    df_merged = pd.concat(dataframes, ignore_index=True)

    log_and_print(logger,f"[PREPROCESADO] Total registros unidos: {len(df_merged)}")

    return df_merged
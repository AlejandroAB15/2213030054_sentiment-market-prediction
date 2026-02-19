from pathlib import Path
from utils.logger import get_logger
from utils.logging_utils import log_and_print
from buildDF_bursatil import build_dataset_prediccion


def run_prediccion():

    logger = get_logger(__name__)

    log_and_print(logger, "[PREDICCION] Iniciando construcción de dataframe con datos bursátiles\n")

    dataset_path = Path("src/data/resultados/dataset_clasificado.json")
    raws_path = Path("src/data/raws")

    df_final = build_dataset_prediccion(
        dataset_path=dataset_path,
        raws_path=raws_path
    )

    log_and_print(
        logger,
        f"[PREDICCION] Dataset construido. Shape: {df_final.shape}"
    )

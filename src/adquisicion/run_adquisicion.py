import json
from pathlib import Path
from utils.logger import setup_logger, get_logger
from utils.logging_utils import log_and_print
from adquisicion.fuentes.el_universal import fetch_base as fetch_universal
from adquisicion.fuentes.el_pais import fetch_base as fetch_pais
from adquisicion.fuentes import indices
from adquisicion.utils.limpieza import limpiar_noticias_por_fecha

RAW_DIR = Path("data/raws")
RAW_DIR.mkdir(parents=True, exist_ok=True)

FECHA_INICIO = "2025-01-01"
FECHA_FIN = "2026-01-31"

def run_adquisicion():
    setup_logger()
    logger = get_logger(__name__)

    log_and_print(logger, "[ADQUISICIÓN] Inicio del proceso")

    log_and_print(logger, "[ADQUISICIÓN] Fetch base de EL UNIVERSAL\n")
    noticias_universal = fetch_universal()

    universal_path = RAW_DIR / "el_universal.json"
    with open(universal_path, "w", encoding="utf-8") as f:
        json.dump(
            noticias_universal,
            f,
            ensure_ascii=False,
            indent=2
        )


    log_and_print(logger, "\n[ADQUISICIÓN] Fetch base de EL PAÍS")
    noticias_pais = fetch_pais()

    noticias_pais = limpiar_noticias_por_fecha(
        noticias_pais,
        FECHA_INICIO,
        FECHA_FIN
    )

    pais_path = RAW_DIR / "el_pais.json"
    with open(pais_path, "w", encoding="utf-8") as f:
        json.dump(
            noticias_pais,
            f,
            ensure_ascii=False,
            indent=2
        )

    log_and_print(
        logger,
        "\n[ADQUISICIÓN] Inicia índices bursátiles"
    )

    datos_indices = indices.descargar_indices(
        FECHA_INICIO,
        FECHA_FIN,
        guardar_csv=True
    )

    for nombre, df in datos_indices.items():
        log_and_print(
            logger,
            f"\n[ADQUISICIÓN][ÍNDICES] {nombre}: "
            f"{len(df)} registros almacenados"
        )

    log_and_print(
        logger,
        "\n[ADQUISICIÓN] Proceso finalizado correctamente"
    )

import json
from pathlib import Path
from utils.logger import setup_logger, get_logger
from utils.logging_utils import log_and_print
from adquisicion.fuentes.el_universal import ( fetch_base as fetch_universal, fetch_contenido_paralelo as contenido_universal )
from adquisicion.fuentes.el_pais import ( fetch_base as fetch_pais, fetch_contenido_paralelo as contenido_pais )
from adquisicion.fuentes.infobae import ( fetch_base as fetch_infobae, fetch_contenido_paralelo as contenido_infobae)
from adquisicion.fuentes.el_financiero import ( fetch_base as fetch_financiero, fetch_contenido_paralelo as contenido_financiero)
from adquisicion.fuentes import indices
from adquisicion.utils.limpieza import limpiar_noticias_por_fecha

RAW_DIR = Path("data/raws")
RAW_DIR.mkdir(parents=True, exist_ok=True)

FECHA_INICIO = "2025-01-01"
FECHA_FIN = "2026-01-31"

def run_el_universal(logger):
    log_and_print(logger, "[ADQUISICIÓN] Fetch base de EL UNIVERSAL\n")

    noticias_universal = fetch_universal()
    noticias_universal = contenido_universal(noticias_universal)

    noticias_universal = limpiar_noticias_por_fecha(
        noticias_universal,
        FECHA_INICIO,
        FECHA_FIN
    )

    universal_path = RAW_DIR / "el_universal.json"
    with open(universal_path, "w", encoding="utf-8") as f:
        json.dump(
            noticias_universal,
            f,
            ensure_ascii=False,
            indent=2
        )

def run_el_pais(logger):
    log_and_print(logger, "\n[ADQUISICIÓN] Fetch base de EL PAÍS")

    noticias_pais = fetch_pais()
    
    noticias_pais = limpiar_noticias_por_fecha(
        noticias_pais,
        FECHA_INICIO,
        FECHA_FIN
    )

    noticias_pais = contenido_pais(noticias_pais)

    pais_path = RAW_DIR / "el_pais.json"
    with open(pais_path, "w", encoding="utf-8") as f:
        json.dump(
            noticias_pais,
            f,
            ensure_ascii=False,
            indent=2
        )

def run_infobae(logger):

    log_and_print(logger, "\n[ADQUISICIÓN] Fetch base de INFOBAE")

    noticias_infobae = fetch_infobae()
    
    noticias_infobae = limpiar_noticias_por_fecha(
        noticias_infobae,
        FECHA_INICIO,
        FECHA_FIN
    )

    noticias_infobae = contenido_infobae(noticias_infobae)

    infobae_path = RAW_DIR / "infobae.json"

    with open(infobae_path, "w", encoding="utf-8") as f:
        json.dump(
            noticias_infobae,
            f,
            ensure_ascii=False,
            indent=2
        )

def run_el_financiero(logger):

    log_and_print(logger, "\n[ADQUISICIÓN] Fetch base de EL_FINANCIERO")

    noticias_financiero = fetch_financiero()
    
    noticias_financiero = limpiar_noticias_por_fecha(
        noticias_financiero,
        FECHA_INICIO,
        FECHA_FIN
    )

    noticias_financiero = contenido_financiero(noticias_financiero)

    el_financiero_path = RAW_DIR / "el_financiero.json"

    with open(el_financiero_path, "w", encoding="utf-8") as f:
        json.dump(
            noticias_financiero,
            f,
            ensure_ascii=False,
            indent=2
        )

def run_indices(logger):
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

def run_adquisicion():
    setup_logger()
    logger = get_logger(__name__)

    log_and_print(logger, "[ADQUISICIÓN] Inicio del proceso")

    # run_el_universal(logger)
    # run_el_pais(logger)
    # run_indices(logger)
    # run_infobae(logger)
    run_el_financiero(logger)

    log_and_print(
        logger,
        "\n[ADQUISICIÓN] Proceso finalizado correctamente"
    )

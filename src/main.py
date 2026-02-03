import json
from pathlib import Path
from utils.logger import setup_logger, get_logger
from utils.logging_utils import log_and_print
from adquisicion.fuentes.el_universal import fetch_base as fetch_universal
from adquisicion.fuentes.el_pais import fetch_base as fetch_pais
from adquisicion.fuentes import indices

RAW_DIR = Path("data/raws")
RAW_DIR.mkdir(parents=True, exist_ok=True)

setup_logger()
logger = get_logger(__name__)

def main():
    log_and_print(logger, "[MAIN] Inicio del proceso")

    log_and_print(logger, "[MAIN] Iniciando fetch base EL UNIVERSAL")
    noticias_universal = fetch_universal()

    universal_path = RAW_DIR / "el_universal_base.json"
    with open(universal_path, "w", encoding="utf-8") as f:
        json.dump(
            noticias_universal,
            f,
            ensure_ascii=False,
            indent=2
        )

    log_and_print(
        logger,
        f"[MAIN] EL UNIVERSAL - Almacenadas ({len(noticias_universal)} noticias)"
    )

    log_and_print(logger, "[MAIN] Iniciando fetch base EL PAÍS")
    noticias_pais = fetch_pais()

    pais_path = RAW_DIR / "el_pais_base.json"
    with open(pais_path, "w", encoding="utf-8") as f:
        json.dump(
            noticias_pais,
            f,
            ensure_ascii=False,
            indent=2
        )

    log_and_print(
        logger,
        f"[MAIN] EL PAÍS - Almacenadas ({len(noticias_pais)} noticias)"
    )

    start_date = "2025-01-01"
    end_date = "2026-01-31"

    log_and_print(
        logger,
        "[MAIN] Iniciando descarga de índices bursátiles"
    )

    datos_indices = indices.descargar_indices(
        start_date,
        end_date,
        guardar_csv=True
    )

    for nombre, df in datos_indices.items():
        log_and_print(
            logger,
            f"[MAIN] {nombre}: {len(df)} registros descargados"
        )

    log_and_print(logger, "[MAIN] Proceso finalizado correctamente")

if __name__ == "__main__":
    main()

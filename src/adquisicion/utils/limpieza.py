from datetime import datetime
from typing import List, Dict
from utils.logger import get_logger
from utils.logging_utils import log_and_print

logger = get_logger(__name__)

def limpiar_noticias_por_fecha(
    noticias: List[Dict],
    fecha_inicio: str,
    fecha_fin: str
) -> List[Dict]:

    inicio = datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
    fin = datetime.strptime(fecha_fin, "%Y-%m-%d").date()
    fuente = noticias[0].get("fuente") if noticias else "desconocida"

    total = len(noticias)
    noticias_limpias = []

    descartadas_sin_fecha = 0
    descartadas_fuera_rango = 0

    log_and_print(
        logger,
        f"[LIMPIEZA][{fuente.upper()}] Iniciando proceso de limpieza"
        f"(rango {fecha_inicio} → {fecha_fin})"
    )

    for noticia in noticias:
        fecha_str = noticia.get("fecha")

        if not fecha_str:
            descartadas_sin_fecha += 1
            continue

        try:
            fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date()
        except Exception:
            descartadas_sin_fecha += 1
            continue

        if inicio <= fecha <= fin:
            noticias_limpias.append(noticia)
        else:
            descartadas_fuera_rango += 1

    log_and_print(
        logger,
        f"[LIMPIEZA] Noticias procesadas: {total}"
    )
    log_and_print(
        logger,
        f"[LIMPIEZA] Noticias válidas: {len(noticias_limpias)}"
    )
    log_and_print(
        logger,
        f"[LIMPIEZA] Descartadas sin fecha válida: {descartadas_sin_fecha}"
    )
    log_and_print(
        logger,
        f"[LIMPIEZA] Descartadas fuera de rango: {descartadas_fuera_rango}"
    )

    return noticias_limpias

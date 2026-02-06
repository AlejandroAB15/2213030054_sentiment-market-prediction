import requests
import json
import time
from utils.logger import get_logger
from utils.logging_utils import log_and_print
from datetime import datetime, timezone
import html
import re
import codecs

logger = get_logger(__name__)

FUENTE = "infobae"

URL_BASE = "https://api.queryly.com/v4/search.aspx"

PARAMS = {
    "queryly_key": "62d9c40063044c14",
    "query": "Trump",
    "endindex": 0,
    "batchsize": 100,
    "callback": "queryly.search.renderAdvancedResults",
    "extendeddatafields": "creator,promo_image",
    "timezoneoffset": 360,
    "initialized": 1
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/141.0.0.0 Safari/537.36 OPR/125.0.0.0"
    ),
    "Accept": "*/*",
    "Referer": "https://www.infobae.com/",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8"
}

DELAY = 1.5

BATCH_SIZE = 25
OFFSET = 150
MAX_BATCHES = 12
START_INDEX = 5


def fetch_base():

    noticias = []

    PARAMS["batchsize"] = BATCH_SIZE

    log_and_print(logger, "[INFOBAE] Iniciando recolección inicial")

    for batch in range(MAX_BATCHES):

        PARAMS["endindex"] = START_INDEX + batch * OFFSET

        try:
            response = requests.get(URL_BASE, params=PARAMS, headers=HEADERS, timeout=10)
            response.raise_for_status()

            text = response.content.decode("utf-8", errors="ignore")

            match = re.search(r"results\s*=\s*JSON\.parse\('(.*?)'\);", text, re.DOTALL)
            if not match:
                raise ValueError("No se pudo extraer JSON principal")

            json_str = match.group(1)
            json_str = codecs.decode(json_str, "unicode_escape")
            json_str = html.unescape(json_str)

            data = json.loads(json_str)

        except Exception as e:
            log_and_print(logger, f"[INFOBAE] Error en batch {batch}: {e}", level="error")
            continue

        items = data.get("items", [])

        if not items:
            log_and_print(logger, f"[INFOBAE] Batch {batch + 1} sin items, fin anticipado", level="warning")
            break

        for item in items:

            link = item.get("link", "")
            url = "https://www.infobae.com" + link

            timestamp = item.get("pubdateunix")
            fecha = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%d/%m/%Y") if timestamp else None

            titulo = item.get("title")
            subtitulo = item.get("description")

            noticias.append({
                "fuente": FUENTE,
                "titulo": titulo.encode("latin1").decode("utf-8") if titulo else None,
                "subtitulo": subtitulo.encode("latin1").decode("utf-8") if subtitulo else None,
                "autor": item.get("creator"),
                "fecha": fecha,
                "url": url,
                "contenido": None
            })

        log_and_print(logger, f"[INFOBAE] Batch {batch + 1}/{MAX_BATCHES} completado ({len(items)} noticias)")

        time.sleep(DELAY)

    log_and_print(logger, f"[INFOBAE] Total noticias obtenidas: {len(noticias)}")

    return noticias

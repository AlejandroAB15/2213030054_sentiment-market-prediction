import requests
import json
import time
from lxml import html
from adquisicion.utils.xpath import get_text
from utils.logger import get_logger
from utils.logging_utils import log_and_print

logger = get_logger(__name__)

FUENTE = "el_universal"

URL_BASE = "https://api.queryly.com/json.aspx"

PARAMS = {
    "queryly_key": "000ca78dce0f4c6b",
    "query": "Trump",
    "endindex": 0,
    "batchsize": 100,
    "callback": "searchPage.resultcallback",
    "showfaceted": "true",
    "extendeddatafields": "creator,imageresizer,promo_image,subheadline",
    "timezoneoffset": 360,
    "facetedkey": "pubDate|",
    "facetedvalue": "8760|"
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/141.0.0.0 Safari/537.36 OPR/125.0.0.0"
    ),
    "Accept": "*/*",
    "Referer": "https://www.eluniversal.com.mx/",
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

    log_and_print(
        logger,
        "[EL_UNIVERSAL] Iniciando recolección inicial"
    )

    for batch in range(MAX_BATCHES):
        PARAMS["endindex"] = START_INDEX + batch * OFFSET

        try:
            response = requests.get(
                URL_BASE,
                params=PARAMS,
                headers=HEADERS,
                timeout=10
            )
            response.raise_for_status()

            text = response.text
            json_str = text[text.find("(") + 1 : text.rfind(");")]
            data = json.loads(json_str)

        except Exception as e:
            log_and_print(
                logger,
                f"[EL_UNIVERSAL] Error en batch {batch}: {e}",
                level="error"
            )
            continue

        items = data.get("items", [])

        if not items:
            log_and_print(
                logger,
                f"[EL_UNIVERSAL] Batch {batch + 1} sin items, fin anticipado",
                level="warning"
            )
            break

        for item in items:
            url = "https://www.eluniversal.com.mx" + item.get("link", "")
            noticias.append({
                "fuente": FUENTE,
                "titulo": item.get("title"),
                "subtitulo": None,
                "autor": item.get("creator"),
                "fecha": None,
                "url": url,
                "contenido": None
            })

        log_and_print(
            logger,
            f"[EL_UNIVERSAL] Batch {batch + 1}/{MAX_BATCHES} completado "
            f"({len(items)} noticias)"
        )

        time.sleep(DELAY)

    log_and_print(
        logger,
        f"[EL_UNIVERSAL] Total noticias obtenidas: {len(noticias)}"
    )

    return noticias


def fetch_contenido(noticia):
    try:
        response = requests.get(
            noticia["url"],
            headers=HEADERS,
            timeout=10
        )
        response.raise_for_status()
        response.encoding = "utf-8"

        tree = html.fromstring(response.text)

        noticia["subtitulo"] = get_text(
            tree,
            '//div[contains(@class,"sc__header")]//h2[contains(@class,"subTitle")]/text()'
        )

        fecha = None
        res = tree.xpath(
            '//div[contains(@class,"sc__author")]'
            '//span[contains(@class,"sc__author--date")]/text()[normalize-space()]'
        )

        for txt in res:
            if "/" in txt and txt.strip():
                fecha = txt.strip().split()[0]
                break

        noticia["fecha"] = fecha

        sections = tree.xpath(
            '//div[contains(@class,"sc") and contains(@class,"pl-3")]//section'
        )

        if not sections:
            log_and_print(
                logger,
                f"[EL_UNIVERSAL] Sin secciones: {noticia['url']}",
                level="warning"
            )
            return noticia

        parrafos = sections[0].xpath(
            './/p[@itemprop="description" and contains(@class,"sc__font-paragraph")]'
        )[:-1]  # quitar firma

        contenido = [
            p.text_content().strip()
            for p in parrafos
            if p.text_content().strip()
            and not p.text_content().strip().lower().startswith("lee también")
            and not p.text_content().strip().lower().startswith("únete a nuestro canal")
        ]

        noticia["contenido"] = " ".join(contenido) if contenido else None

        time.sleep(DELAY)

    except Exception as e:
        log_and_print(
            logger,
            f"[EL_UNIVERSAL] Error contenido: {noticia['url']} -> {e}",
            level="error"
        )
        noticia["contenido"] = None

    return noticia

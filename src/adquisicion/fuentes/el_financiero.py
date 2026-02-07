import requests, json, time, re, codecs, html
from datetime import datetime, timezone
from utils.logger import get_logger
from utils.logging_utils import log_and_print
from lxml import html as lxml_html
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = get_logger(__name__)

FUENTE = "el_financiero"
URL_BASE = "https://api.queryly.com/v4/search.aspx"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"),
    "Accept": "*/*",
    "Referer": "https://www.elfinanciero.com.mx/",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8"
}

PARAMS = {
    "queryly_key": "327f7c9cf1384818",
    "query": "Trump",
    "endindex": 0,
    "batchsize": 0,
    "callback": "queryly.search.renderAdvancedResults",
    "extendeddatafields": "creator,promo_image",
    "timezoneoffset": 360,
    "initialized": 1
}

DELAY = 1.5
START_INDEX = 0
OFFSET = 160
MAX_BATCHES = 20
BATCH_SIZE = 30

def fetch_base():

    noticias = []
    PARAMS["batchsize"] = BATCH_SIZE

    log_and_print(logger, "[EL_FINANCIERO] Iniciando recolección inicial")

    EXCLUIR = ("/opinion/", "/columnas/", "/blogs/")

    for batch in range(MAX_BATCHES):

        PARAMS["endindex"] = START_INDEX + batch * OFFSET

        try:
            response = requests.get(URL_BASE, params=PARAMS, headers=HEADERS, timeout=10)
            response.raise_for_status()

            text = response.content.decode("utf-8", errors="ignore")

            match = re.search(r"results\s*=\s*JSON\.parse\('(.*?)'\);", text, re.DOTALL)
            if not match: raise ValueError("No se pudo extraer JSON principal")

            json_str = match.group(1)
            json_str = codecs.decode(json_str, "unicode_escape")
            json_str = html.unescape(json_str)

            data = json.loads(json_str)

        except Exception as e:
            log_and_print(logger, f"[EL_FINANCIERO] Error en batch {batch}: {e}", level="error")
            continue

        items = data.get("items", [])

        if not items:
            log_and_print(logger, f"[EL_FINANCIERO] Batch {batch + 1} sin items, fin anticipado", level="warning")
            break

        for item in items:

            link = item.get("link", "")
            url = "https://www.elfinanciero.com.mx" + link if link else None

            if not url or any(x in url for x in EXCLUIR):
                continue

            timestamp = item.get("pubdateunix")
            fecha = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%d/%m/%Y") if timestamp else None

            titulo = item.get("title")
            subtitulo = item.get("description")

            try: titulo = titulo.encode("latin1").decode("utf-8") if titulo else None
            except UnicodeDecodeError: pass

            try: subtitulo = subtitulo.encode("latin1").decode("utf-8") if subtitulo else None
            except UnicodeDecodeError: pass

            noticias.append({
                "fuente": FUENTE,
                "titulo": titulo,
                "subtitulo": subtitulo,
                "autor": item.get("creator"),
                "fecha": fecha,
                "url": url,
                "contenido": None
            })

        log_and_print(
            logger,
            f"[EL_FINANCIERO] Batch {batch + 1}/{MAX_BATCHES} completado"
            f"({len(items)} noticias)"
        )

        time.sleep(DELAY)

    log_and_print(logger, f"[EL_FINANCIERO] Total noticias obtenidas: {len(noticias)}")

    return noticias


def fetch_contenido(noticia):

    try:
        response = requests.get(noticia["url"], headers=HEADERS, timeout=10)
        response.raise_for_status()
        response.encoding = "utf-8"

        tree = lxml_html.fromstring(response.text)

        body = tree.xpath('//article[contains(@class,"article-body-wrapper")]')
        parrafos = body[0].xpath('.//p[contains(@class,"c-paragraph")]')

        contenido_lista = [
            p.text_content().strip()
            for p in parrafos
            if p.text_content().strip()
        ]

        contenido_lista = contenido_lista[:-1]  # Al igual que en el universal, el último parrafo no suele ser útil

        noticia["contenido"] = " ".join(contenido_lista) if contenido_lista else None

        time.sleep(DELAY)

    except Exception as e:
        log_and_print(logger, f"[EL_FINANCIERO] Error contenido: {noticia['url']} -> {e}", level="error")
        noticia["contenido"] = None

    return noticia

def fetch_contenido_paralelo(noticias, max_workers=4):

    log_and_print(logger, f"\n[EL_FINANCIERO] Fetch contenido paralelo ({max_workers} workers)")

    noticias_completas = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        future_to_url = {
            executor.submit(fetch_contenido, noticia): noticia["url"]
            for noticia in noticias
        }

        for future in as_completed(future_to_url):
            url = future_to_url[future]

            try:
                noticia = future.result()
                noticias_completas.append(noticia)

            except Exception as e:
                log_and_print(logger, f"[EL_FINANCIERO] Error thread {url}: {e}", level="error")

    log_and_print(logger, f"[EL_FINANCIERO] Contenido completado ({len(noticias_completas)})")

    return noticias_completas
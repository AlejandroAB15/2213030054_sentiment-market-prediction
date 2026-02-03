import time
import requests
from lxml import html
from adquisicion.utils.xpath import get_text
from utils.logger import get_logger
from utils.logging_utils import log_and_print

logger = get_logger(__name__)

FUENTE = "elpais"

URL_BASE = "https://elpais.com/buscador/Trump/{}"
DELAY = 1.2

PAGINAS_POR_BLOQUE = 4
PAGE_OFFSET = 50
MAX_OFFSET = 500
START_PAGE = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/141.0.0.0 Safari/537.36 OPR/125.0.0.0"
    ),
    "Referer": "https://elpais.com/mexico/",
    "Upgrade-Insecure-Requests": "1",
}

def fetch_base():
    noticias = []

    bloques = list(range(0, MAX_OFFSET + 1, PAGE_OFFSET))
    total_bloques = len(bloques)

    log_and_print(
        logger,
        "[EL_PAIS] Iniciando recolección inicial"
    )

    for i, offset in enumerate(bloques):
        paginas = range(
            START_PAGE + offset,
            START_PAGE + offset + PAGINAS_POR_BLOQUE
        )

        log_and_print(
            logger,
            f"[EL_PAIS] Bloque {i + 1}/{total_bloques} iniciado"
        )

        total_articulos_bloque = 0

        for pagina in paginas:
            try:
                url = URL_BASE.format(pagina)
                response = requests.get(
                    url,
                    headers=HEADERS,
                    timeout=10
                )
                response.raise_for_status()

                tree = html.fromstring(response.text)

                articulos = tree.xpath(
                    '//section[@id="search-results"]'
                    '//div[@id="results-container"]//article'
                )

            except Exception as e:
                log_and_print(
                    logger,
                    f"[EL_PAIS] Error en página {pagina}: {e}",
                    level="error"
                )
                continue

            for articulo in articulos:
                link = articulo.xpath('.//h2//a/@href')
                if not link:
                    continue

                noticias.append({
                    "fuente": FUENTE,
                    "titulo": get_text(articulo, './/h2//a/text()'),
                    "subtitulo": None,
                    "autor": get_text(
                        articulo,
                        './/div[contains(@class,"c_a")]//a/text()'
                    ),
                    "fecha": get_text(
                        articulo,
                        './/div[contains(@class,"c_f")]/text()'
                    ),
                    "url": link[0],
                    "contenido": None
                })

            total_articulos_bloque += len(articulos)
            time.sleep(DELAY)

        log_and_print(
            logger,
            f"[EL_PAIS] Bloque {i + 1}/{total_bloques} completado "
            f"({total_articulos_bloque} artículos)"
        )

    log_and_print(
        logger,
        f"[EL_PAIS] Total noticias obtenidas: {len(noticias)}"
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

        tree = html.fromstring(response.text)

        parrafos = tree.xpath(
            '//article[@id="main-content"]'
            '//div[contains(@class,"a_c")]//p'
        )

        contenido = [
            p.text_content().strip()
            for p in parrafos
            if p.text_content().strip()
        ]

        noticia["contenido"] = " ".join(contenido) if contenido else None
        time.sleep(DELAY)

    except Exception as e:
        log_and_print(
            logger,
            f"[EL_PAIS] Fallo contenido: {noticia['url']} -> {e}",
            level="error"
        )
        noticia["contenido"] = None

    return noticia

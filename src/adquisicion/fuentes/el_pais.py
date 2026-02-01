import time
import requests
from lxml import html
from adquisicion.utils.xpath import get_text

FUENTE = "elpais"

URL_BASE = "https://elpais.com/buscador/Trump/{}"
DELAY = 1.2

PAGINAS_POR_BLOQUE = 4
PAGE_OFFSET = 50
MAX_OFFSET = 500

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

    for offset in range(0, MAX_OFFSET + 1, PAGE_OFFSET):
        paginas = range(
            offset + 1,
            offset + PAGINAS_POR_BLOQUE + 1
        )

        for pagina in paginas:
            try:
                url = URL_BASE.format(pagina)
                response = requests.get(url, headers=HEADERS, timeout=10)
                response.raise_for_status()

                tree = html.fromstring(response.text)

                articulos = tree.xpath(
                    '//section[@id="search-results"]'
                    '//div[@id="results-container"]//article'
                )

            except Exception as e:
                print(f"[EL_PAIS] Error en página {pagina}: {e}")
                continue

            for articulo in articulos:
                link = articulo.xpath('.//h2//a/@href')
                if not link:
                    continue

                noticias.append({
                    "fuente": FUENTE,
                    "titulo": get_text(articulo, './/h2//a/text()'),
                    "subtitulo": None,
                    "autor": get_text(articulo, './/div[contains(@class,"c_a")]//a/text()'),
                    "fecha": get_text(articulo, './/div[contains(@class,"c_f")]/text()'),
                    "url": link[0],
                    "contenido": None
                })

            print(f"[EL_PAIS] Página {pagina} -> {len(articulos)} artículos encontrados")

    return noticias


def fetch_contenido(noticia):
    try:
        response = requests.get(noticia["url"], headers=HEADERS, timeout=10)
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
        print(f"[EL_PAIS] Fallo contenido: {noticia['url']} -> {e}")
        noticia["contenido"] = None

    return noticia

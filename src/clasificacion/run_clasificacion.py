import json
import time
import re
import requests
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.logger import get_logger
from utils.logging_utils import log_and_print

MODEL = "llama3.1:8b"
OLLAMA_URL = "http://localhost:11434/api/generate"
MAX_WORKERS = 3
MAX_RETRIES = 3
TIMEOUT = 120


def clasificar_llama(texto: str) -> str:

    prompt = f"""
        Eres un analista económico.

        Analiza el siguiente texto periodístico y clasifica el impacto económico esperado 
        sobre mercados financieros derivado de las acciones, políticas o decisiones descritas.

        Criterios:

        - Si las acciones descritas aumentan riesgo económico, tensiones comerciales, 
        incertidumbre regulatoria o costos → NEG
        - Si las acciones descritas estimulan inversión, reducen riesgo o favorecen 
        actividad económica → POS
        - Si el texto es principalmente editorial, satírico, opinativo o no describe 
        una acción económica concreta → NEU
        - No inferir impacto económico únicamente por tono negativo o polémico
        - Ignorar lenguaje figurativo u ofensivo

        No evalúes ideología ni postura política.
        No agregues explicación.

        Responde únicamente en formato JSON:
        {{"label": "POS"}}

        Texto:
        {texto}
        """

    # Se intenta clasificar un número máximo de veces
    for intento in range(MAX_RETRIES):
        try:
            with requests.Session() as session:
                # Solicitud al modelo local
                response = session.post(
                    OLLAMA_URL,
                    json={
                        "model": MODEL,
                        "prompt": prompt,
                        "temperature": 0,
                        "stream": False
                    },
                    timeout=TIMEOUT
                )

            if response.status_code != 200:
                raise RuntimeError(f"Status {response.status_code}")

            # El modelo regresa una respuesta en formato JSON, esta se debe de procesar para obtener la etiqueta
            raw = response.json().get("response", "").strip()
            match = re.search(r'\{.*?\}', raw, re.DOTALL)

            if match:
                try:
                    label = json.loads(match.group(0))["label"]
                    if label in {"POS", "NEG", "NEU"}:
                        return label
                except Exception:
                    return "ERROR"

            return "ERROR"

        # Si hay errores se manda a esperar un momento y se hace un retry
        except Exception:
            if intento == MAX_RETRIES - 1:
                return "ERROR"
            time.sleep(2 ** intento)

    return "ERROR"


def run_clasificacion():

    logger = get_logger(__name__)
    log_and_print(logger, "\n[CLASIFICACION] Inicio de clasificación")

    input_path = Path("data/procesados/dataset_relevante.json")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    total = len(df)
    log_and_print(logger, f"[CLASIFICACION] Registros a clasificar: {total}")

    labels = [None] * total

    # Se ejecutan en paralelo las solicitudes al modelo con 3 'hilos' simultaneos
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {
            executor.submit(clasificar_llama, row["contenido"]): idx
            for idx, row in df.iterrows()
        }

        for i, future in enumerate(as_completed(futures), start=1):

            idx = futures[future]
            labels[idx] = future.result()

            if i % 25 == 0 or i == total:
                log_and_print(
                    logger,
                    f"[CLASIFICACION] Progreso: {i}/{total}"
                )

    df["sentimiento_label"] = labels

    columnas_finales = [
        "titulo",
        "subtitulo",
        "fecha",
        "fuente",
        "sentimiento_label"
    ]

    df_final = df[columnas_finales]

    output_dir = Path("data/resultados")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "dataset_clasificado.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            df_final.to_dict("records"),
            f,
            ensure_ascii=False,
            indent=2
        )

    log_and_print(
        logger,
        "[CLASIFICACION] Clasificación completada y exportada"
    )
import json
import torch
import pandas as pd
from pathlib import Path
from transformers import pipeline
from utils.logger import get_logger
from utils.logging_utils import log_and_print

MODELO_SENTIMIENTO = "finiteautomata/beto-sentiment-analysis"

def run_clasificacion():

    logger = get_logger(__name__)
    log_and_print(logger, "\n[CLASIFICACION] Inicio de clasificación")

    datos_iniciales = Path("data/procesados/dataset_relevante.json")

    if not datos_iniciales.exists():
        raise FileNotFoundError(
            "[CLASIFICACION] No se encontró dataset_relevante.json"
        )

    with open(datos_iniciales, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    log_and_print(
        logger,
        f"[CLASIFICACION] Registros a clasificar: {len(df)}"
    )

    device = 0 if torch.cuda.is_available() else -1

    clasificador = pipeline(
        "sentiment-analysis",
        model=MODELO_SENTIMIENTO,
        tokenizer=MODELO_SENTIMIENTO,
        device=device
    )

    resultados = clasificador(
        df["contenido"].tolist(),
        batch_size=32,
        truncation=True,
        max_length=512
    )

    df["sentimiento_label"] = [r["label"] for r in resultados]
    df["sentimiento_score"] = [r["score"] for r in resultados]

    columnas_finales = [
        "titulo",
        "subtitulo",
        "fecha",
        "fuente",
        "sentimiento_label",
        "sentimiento_score"
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

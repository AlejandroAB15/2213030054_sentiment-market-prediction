import json
import torch
import pandas as pd
from pathlib import Path
from pysentimiento import create_analyzer
from utils.logger import get_logger
from utils.logging_utils import log_and_print


def run_clasificacion():

    logger = get_logger(__name__)
    log_and_print(logger, "\n[CLASIFICACION] Inicio de clasificación")

    datos_iniciales = Path("data/procesados/dataset_relevante.json")

    with open(datos_iniciales, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    log_and_print(
        logger,
        f"[CLASIFICACION] Registros a clasificar: {len(df)}"
    )

    device = "cuda" if torch.cuda.is_available() else "cpu"

    analyzer = create_analyzer(
        task="sentiment",
        lang="es",
        device=device
    )

    labels = []
    scores = []

    for texto in df["contenido"]:
        result = analyzer.predict(texto)
        labels.append(result.output)
        scores.append(float(result.probas[result.output]))

    df["sentimiento_label"] = labels
    df["sentimiento_score"] = scores

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
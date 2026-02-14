import json
from pathlib import Path
from utils.logger import get_logger
from utils.logging_utils import log_and_print
from preprocesado.union_fuentes import unir_datasets
from preprocesado.saneamiento import aplicar_saneamiento_base
from preprocesado.filtrado_relevancia import filtrar_relevancia_trump
from preprocesado.deduplicado import deduplicar_datasets

def run_preprocesado():

    logger = get_logger(__name__)

    log_and_print(logger, "\n[PREPROCESADO] Inicio del pipeline de preprocesamiento")

    sources = {
        "infobae": "data/raws/infobae.json",
        "el_financiero": "data/raws/el_financiero.json",
        "el_pais": "data/raws/el_pais.json",
        "el_universal": "data/raws/el_universal.json",
    }

    df = unir_datasets(sources, logger)

    df = aplicar_saneamiento_base(df, logger)

    df_relevantes, df_no_relevantes = filtrar_relevancia_trump(df, logger)

    df_relevantes, df_no_relevantes = deduplicar_datasets(
        df_relevantes,
        df_no_relevantes,
        logger
    )

    columnas_aux = [
        "menciona_trump_titulo",
        "menciones_trump_body",
        "es_relevante_trump"
    ]

    # Se quitan las columnas auxiliares
    df_relevantes = df_relevantes.drop(columns=columnas_aux, errors="ignore")
    df_no_relevantes = df_no_relevantes.drop(columns=columnas_aux, errors="ignore")

    # 6. Formato final de fecha (dd-mm-aaaa)
    df_relevantes["fecha"] = df_relevantes["fecha"].dt.strftime("%d-%m-%Y")
    df_no_relevantes["fecha"] = df_no_relevantes["fecha"].dt.strftime("%d-%m-%Y")

    output_dir = Path("data/procesados")
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_dir / "dataset_relevante.json", "w", encoding="utf-8") as f:
        json.dump(
            df_relevantes.to_dict("records"),
            f,
            ensure_ascii=False,
            indent=2
        )

    with open(output_dir / "dataset_no_relevante.json", "w", encoding="utf-8") as f:
        json.dump(
            df_no_relevantes.to_dict("records"),
            f,
            ensure_ascii=False,
            indent=2
        )

    log_and_print(logger, "[PREPROCESADO] Datasets exportados correctamente")

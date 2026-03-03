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

    resumen = {}

    sources = {
        "infobae": "data/raws/infobae.json",
        "el_financiero": "data/raws/el_financiero.json",
        "el_pais": "data/raws/el_pais.json",
        "el_universal": "data/raws/el_universal.json",
    }

    df_union = unir_datasets(sources, logger)

    total_original = len(df_union)

    resumen["union"] = {
        "total_original": total_original,
        "por_fuente": df_union["fuente"].value_counts().to_dict()
    }

    total_antes_saneamiento = len(df_union)

    df_saneado = aplicar_saneamiento_base(df_union, logger)

    resumen["saneamiento"] = {
        "registros_validos": len(df_saneado),
        "registros_eliminados": total_antes_saneamiento - len(df_saneado)
    }

    df_relevantes, df_no_relevantes = filtrar_relevancia_trump(
        df_saneado,
        logger
    )

    resumen["relevancia"] = {
        "relevantes": len(df_relevantes),
        "no_relevantes": len(df_no_relevantes)
    }

    antes_rel = len(df_relevantes)
    antes_no_rel = len(df_no_relevantes)

    df_relevantes, df_no_relevantes = deduplicar_datasets(
        df_relevantes,
        df_no_relevantes,
        logger
    )

    resumen["deduplicacion"] = {
        "duplicados_relevantes": antes_rel - len(df_relevantes),
        "duplicados_no_relevantes": antes_no_rel - len(df_no_relevantes)
    }

    total_final_relevantes = len(df_relevantes)

    porcentaje_reduccion_total = (
        (total_original - total_final_relevantes)
        / total_original
        * 100
    )

    resumen["resumen_final"] = {
        "total_final_relevantes": total_final_relevantes,
        "porcentaje_reduccion_total": round(porcentaje_reduccion_total, 2)
    }

    columnas_aux = [
        "menciona_trump_titulo",
        "menciones_trump_body",
        "es_relevante_trump"
    ]

    df_relevantes = df_relevantes.drop(columns=columnas_aux, errors="ignore")
    df_no_relevantes = df_no_relevantes.drop(columns=columnas_aux, errors="ignore")

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
    log_and_print(logger, "[PREPROCESADO] Resumen estructurado generado")

    return df_relevantes, df_no_relevantes, resumen
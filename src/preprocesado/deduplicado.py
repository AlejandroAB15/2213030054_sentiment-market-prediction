import pandas as pd
from utils.logging_utils import log_and_print

def deduplicar_datasets(
    df_relevantes: pd.DataFrame,
    df_no_relevantes: pd.DataFrame,
    logger
) -> tuple[pd.DataFrame, pd.DataFrame]:

    log_and_print(logger,"\n[PREPROCESADO] Inicio de deduplicación de datasets")

    antes_rel = len(df_relevantes)

    df_relevantes = (
        df_relevantes
        .drop_duplicates(subset=["titulo"])
        .reset_index(drop=True)
    )

    despues_rel = len(df_relevantes)

    log_and_print(logger, f"[PREPROCESADO] Cantidad de duplicados relevantes eliminados: {antes_rel - despues_rel}")

    antes_no_rel = len(df_no_relevantes)

    df_no_relevantes = (
        df_no_relevantes
        .drop_duplicates(subset=["titulo"])
        .reset_index(drop=True)
    )

    despues_no_rel = len(df_no_relevantes)

    log_and_print(
        logger,
        f"[PREPROCESADO] Cantidad de duplicados no relevantes eliminados: {antes_no_rel - despues_no_rel}"
    )

    return df_relevantes, df_no_relevantes

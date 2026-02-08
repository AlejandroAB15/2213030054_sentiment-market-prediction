import re
import pandas as pd
from utils.logging_utils import log_and_print

def filtrar_relevancia_trump(
    df: pd.DataFrame,
    logger,
    min_menciones_body: int = 3
) -> tuple[pd.DataFrame, pd.DataFrame]:

    log_and_print(logger, "\n[PREPROCESADO] Inicio de filtro de relevancia temática")

    df = df.copy()

    df["menciones_trump_body"] = df["contenido"].apply(
        lambda x: 0 if pd.isna(x)
        else len(re.findall(r"\btrump\b", str(x), re.IGNORECASE))
    )

    df["es_relevante_trump"] = (df["menciona_trump_titulo"] | (df["menciones_trump_body"] >= min_menciones_body))

    log_and_print(
        logger,
        f"[PREPROCESADO] Relevantes: {df['es_relevante_trump'].sum()} | "
        f"A revisar: {(~df['es_relevante_trump']).sum()}"
    )

    return (
        df[df["es_relevante_trump"]].reset_index(drop=True),
        df[~df["es_relevante_trump"]].reset_index(drop=True)
    )

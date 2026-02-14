import re
import pandas as pd
from utils.logging_utils import log_and_print
from clasificacion.utils.token_truncation import BetoTokenTruncator

def aplicar_saneamiento_base(
    df: pd.DataFrame,
    logger
) -> pd.DataFrame:

    log_and_print(logger, "\n[PREPROCESADO] Inicio de saneamiento base del dataframe")

    df = df.copy()

    df["contenido"] = df["contenido"].astype(str).str.strip()

    # Se eliminan registros sin contenido
    df = df[
        (df["contenido"].notna()) &
        (df["contenido"] != "") &
        (df["contenido"].str.lower() != "nan")
    ]

    log_and_print(
        logger,
        f"[PREPROCESADO] Registros tras eliminar contenidos nulos: {len(df)}"
    )

    # Flag para checar si el título menciona a Trump
    patron_trump = re.compile(r"\btrump\b", flags=re.IGNORECASE)

    df["menciona_trump_titulo"] = df["titulo"].apply(
        lambda x: bool(patron_trump.search(str(x)))
    )

    # Truncamiento alineado con BETO
    log_and_print(
        logger,
        "[PREPROCESADO] Aplicando truncamiento basado en tokenizer BETO (512 tokens)"
    )

    truncator = BetoTokenTruncator()

    df["contenido"] = df["contenido"].apply(truncator.truncate_to_model_limit)

    return df

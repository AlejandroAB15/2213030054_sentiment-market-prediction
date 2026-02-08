import re
import pandas as pd
from utils.logging_utils import log_and_print

def aplicar_saneamiento_base(
    df: pd.DataFrame,
    logger,
    max_chars: int = 2000
) -> pd.DataFrame:

    log_and_print(logger, "\n[PREPROCESADO] Inicio de saneamiento base del dataframe")

    df = df.copy()

    df["contenido"] = df["contenido"].astype(str)

    df["contenido"] = df["contenido"].str.strip()

    df = df[
        (df["contenido"].notna()) &
        (df["contenido"] != "") &
        (df["contenido"].str.lower() != "nan")
    ]

    log_and_print(logger,f"[PREPROCESADO] Registros tras eliminar contenidos nulos: {len(df)}")

    ## Flags para verificar si el titulo menciona a trump
    ## Para tener en cuenta que registros verificar en el siguiente paso
    patron_trump = re.compile(
        r"\btrump\b",
        flags=re.IGNORECASE
    )

    df["menciona_trump_titulo"] = df["titulo"].apply(
        lambda x: bool(
            patron_trump.search(str(x))
        )
    )

    df["contenido"] = df["contenido"].str.slice(
        0,
        max_chars
    )

    log_and_print(logger,f"[PREPROCESADO] Contenido truncado a máximo de {max_chars} caracteres")

    return df

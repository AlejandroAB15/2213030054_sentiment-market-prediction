import pandas as pd


def construir_variables_modelo(
    df: pd.DataFrame,
    indice_base: str,
    ventana: int,
    usar_futuro: bool = True # Indica si se toman en cuenta en el calculo los valores de close a 7 dias
):

    df = df.copy()

    close_t = f"close_{indice_base}_t"
    close_t7 = f"close_{indice_base}_t7"

    df["MM_close_t"] = df[close_t].rolling(
        window=ventana,
        min_periods=ventana
    ).mean()

    df["MM_T1"] = df["T1"].rolling(
        window=ventana,
        min_periods=ventana
    ).mean()

    df["MM_T2"] = df["T2"].rolling(
        window=ventana,
        min_periods=ventana
    ).mean()

    df["MM_T3"] = df["T3"].rolling(
        window=ventana,
        min_periods=ventana
    ).mean()

    if usar_futuro:

        df["MM_close_t7"] = df[close_t7].rolling(
            window=ventana,
            min_periods=ventana
        ).mean()

        df["P"] = (
            df["MM_close_t"] +
            df["MM_close_t7"]
        ) / 2

    else:

        df["P"] = df["MM_close_t"]

    df["T_prom"] = (
        df["MM_T1"] +
        df["MM_T2"] +
        df["MM_T3"]
    ) / 3

    df = df.dropna()

    return df
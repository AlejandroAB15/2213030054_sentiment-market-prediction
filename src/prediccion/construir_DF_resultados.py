import pandas as pd

def construir_dataset_resultados(
    df_general: pd.DataFrame,
    df_especifico: pd.DataFrame,
    indice: str
) -> pd.DataFrame:

    close_col = f"close_{indice}_t"
    close7_col = f"close_{indice}_t7"

    df = pd.DataFrame()

    df["fecha"] = df_general["fecha"]
    df["indice"] = indice

    df["close_real"] = df_general[close_col]
    df["close_real_7"] = df_general[close7_col]

    df["pred_general"] = df_general["pred_close_modelo"]
    df["pred_general_7"] = df_general["pred_close7_modelo"]

    df["pred_especifico"] = df_especifico["pred_close_modelo"]
    df["pred_especifico_7"] = df_especifico["pred_close7_modelo"]

    df["error_general"] = df["pred_general"] - df["close_real"]
    df["error_especifico"] = df["pred_especifico"] - df["close_real"]

    # separación entre datos de entrenamiento y de prueba
    limite = int(len(df) * 0.8)

    df["segmento"] = "train"
    df.loc[df.index >= limite, "segmento"] = "test"

    return df.reset_index(drop=True)
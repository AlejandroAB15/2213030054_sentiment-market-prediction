import pandas as pd

from prediccion.regresion_lineal import (
    construir_matriz_X,
    construir_vector_y,
    calcular_beta,
    predecir
)

def ejecutar_modelo_especifico(df_variables: pd.DataFrame, indice: str):

    limite = int(len(df_variables) * 0.8)

    df_test = df_variables.iloc[:limite].copy()

    columnas_modelo = [
        "P",
        "MM_T1",
        "MM_T2",
        "MM_T3"
    ]

    close_t = f"close_{indice}_t"
    close_t7 = f"close_{indice}_t7"

    x_test = construir_matriz_X(
        df_test,
        columnas_modelo
    )

    y_train_close = construir_vector_y(
        df_test,
        close_t
    )

    beta_close = calcular_beta(x_test, y_train_close)

    y_train_close7 = construir_vector_y(
        df_test,
        close_t7
    )

    beta_close7 = calcular_beta(x_test, y_train_close7)

    X_total = construir_matriz_X(
        df_variables,
        columnas_modelo
    )

    pred_close = predecir(X_total, beta_close)
    pred_close7 = predecir(X_total, beta_close7)

    df_resultado = df_variables.copy()

    df_resultado["pred_close_modelo"] = pred_close
    df_resultado["pred_close7_modelo"] = pred_close7

    return df_resultado
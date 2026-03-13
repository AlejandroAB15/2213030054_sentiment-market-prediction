import pandas as pd

from prediccion.regresion_lineal import (
    construir_matriz_X,
    construir_vector_y,
    calcular_beta,
    predecir
)


def ejecutar_modelo_general(df_variables: pd.DataFrame, indice: str):

    limite = int(len(df_variables) * 0.8)

    df_train = df_variables.iloc[:limite].copy()

    columnas_modelo = ["P", "T_prom"]

    x_test = construir_matriz_X(
        df_train,
        columnas_modelo
    )

    close_t = f"close_{indice}_t"
    close_t7 = f"close_{indice}_t7"

    # modelo close normal
    y_train_close = construir_vector_y(
        df_train,
        close_t
    )

    # modelo close a 7 días
    y_train_close7 = construir_vector_y(
        df_train,
        close_t7
    )

    beta_close = calcular_beta(x_test, y_train_close)
    beta_close7 = calcular_beta(x_test, y_train_close7)

    # predicciones para todo el dataset
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
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import random
from prediccion.componentes_modelo import calcular_componentes_modelo
from prediccion.variables_modelo import construir_variables_modelo
from prediccion.modelos.modelo_general import ejecutar_modelo_general
from prediccion.modelos.modelo_especifico import ejecutar_modelo_especifico

def calcular_error_test(df, real_col, pred_col):

    limite = int(len(df) * 0.8)

    df_test = df.iloc[limite:]

    real = df_test[real_col]
    pred = df_test[pred_col]

    return (abs((real - pred) / real)).mean() * 100

def graficar_modelo(df, real_col, pred_col, titulo):

    limite = int(len(df) * 0.8)

    df_train = df.iloc[:limite]
    df_test = df.iloc[limite:]

    plt.figure(figsize=(12,6))

    plt.plot(
        df["fecha"],
        df[real_col],
        color="black",
        linewidth=2,
        label="Datos reales"
    )

    plt.plot(
        df_train["fecha"],
        df_train[pred_col],
        linestyle="--",
        color="tab:blue",
        label="Predicción entrenamiento"
    )

    plt.plot(
        df_test["fecha"],
        df_test[pred_col],
        linestyle="--",
        color="tab:orange",
        label="Predicción prueba"
    )

    plt.title(titulo)
    plt.xlabel("Fecha")
    plt.ylabel("Precio")

    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.show()

def seleccionar_periodo_trimestre():

    inicio = pd.Timestamp("2025-01-01")
    fin = pd.Timestamp("2026-01-31")

    margen = 90

    rango_inicio = inicio + pd.Timedelta(days=margen)
    rango_fin = fin - pd.Timedelta(days=margen)

    dias_disponibles = (rango_fin - rango_inicio).days

    offset = random.randint(0, dias_disponibles - 90)

    fecha_inicio = rango_inicio + pd.Timedelta(days=offset)

    return fecha_inicio.strftime("%Y-%m-%d")

def evaluar_modelos_por_indice(df_model_valido: pd.DataFrame, indice: str):

    fecha_inicio = seleccionar_periodo_trimestre()

    print(f"Indice: {indice.upper()}")
    print(f"Periodo de 90 días desde {fecha_inicio}")

    df_componentes = calcular_componentes_modelo(
        df=df_model_valido,
        indice_base=indice,
        fecha_inicio=fecha_inicio,
        n_dias=90,
        ventana=7
    )

    mejor_general_con_futuro = None
    mejor_general_sin_futuro = None
    mejor_especifico_con_futuro = None
    mejor_especifico_sin_futuro = None

    mejor_error_general_con_futuro = float("inf")
    mejor_error_general_sin_futuro = float("inf")
    mejor_error_especifico_con_futuro = float("inf")
    mejor_error_especifico_sin_futuro = float("inf")

    mejor_ventana_general_con_futuro = None
    mejor_ventana_general_sin_futuro = None
    mejor_ventana_especifico_con_futuro = None
    mejor_ventana_especifico_sin_futuro = None

    real_col = f"close_{indice}_t7"

    for usar_futuro in [True, False]:

        for ventana in range(1, 11):

            df_variables = construir_variables_modelo(
                df_componentes,
                indice_base=indice,
                ventana=ventana,
                usar_futuro=usar_futuro
            )

            if len(df_variables) < 10:
                continue

            # ---------------- GENERAL ----------------
            df_general = ejecutar_modelo_general(df_variables, indice)

            pred_col = "pred_close7_modelo"

            error_test = calcular_error_test(
                df_general,
                real_col,
                pred_col
            )

            if usar_futuro:
                if error_test < mejor_error_general_con_futuro:
                    mejor_error_general_con_futuro = error_test
                    mejor_general_con_futuro = df_general
                    mejor_ventana_general_con_futuro = ventana
            else:
                if error_test < mejor_error_general_sin_futuro:
                    mejor_error_general_sin_futuro = error_test
                    mejor_general_sin_futuro = df_general
                    mejor_ventana_general_sin_futuro = ventana

            # ---------------- ESPECIFICO ----------------
            df_especifico = ejecutar_modelo_especifico(df_variables, indice)

            error_test = calcular_error_test(
                df_especifico,
                real_col,
                pred_col
            )

            if usar_futuro:
                if error_test < mejor_error_especifico_con_futuro:
                    mejor_error_especifico_con_futuro = error_test
                    mejor_especifico_con_futuro = df_especifico
                    mejor_ventana_especifico_con_futuro = ventana
            else:
                if error_test < mejor_error_especifico_sin_futuro:
                    mejor_error_especifico_sin_futuro = error_test
                    mejor_especifico_sin_futuro = df_especifico
                    mejor_ventana_especifico_sin_futuro = ventana

    graficar_modelo(
        mejor_general_con_futuro,
        real_col,
        "pred_close7_modelo",
        f"{indice.upper()} - modelo CON futuro"
    )

    graficar_modelo(
        mejor_general_sin_futuro,
        real_col,
        "pred_close7_modelo",
        f"{indice.upper()} - modelo SIN futuro"
    )

    return {
        "general_con_futuro": {
            "df": mejor_general_con_futuro,
            "mape": mejor_error_general_con_futuro,
            "ventana": mejor_ventana_general_con_futuro
        },
        "general_sin_futuro": {
            "df": mejor_general_sin_futuro,
            "mape": mejor_error_general_sin_futuro,
            "ventana": mejor_ventana_general_sin_futuro
        },
        "especifico_con_futuro": {
            "df": mejor_especifico_con_futuro,
            "mape": mejor_error_especifico_con_futuro,
            "ventana": mejor_ventana_especifico_con_futuro
        },
        "especifico_sin_futuro": {
            "df": mejor_especifico_sin_futuro,
            "mape": mejor_error_especifico_sin_futuro,
            "ventana": mejor_ventana_especifico_sin_futuro
        }
    }

def seleccionar_mejor_modelo(resultados, tipo):

    general = resultados[f"general_{tipo}"]
    especifico = resultados[f"especifico_{tipo}"]

    if general["mape"] < especifico["mape"]:
        return {
            "tipo": "general",
            "mape": general["mape"],
            "ventana": general["ventana"]
        }
    else:
        return {
            "tipo": "especifico",
            "mape": especifico["mape"],
            "ventana": especifico["ventana"]
        }

def construir_resumen_indice(indice, resultados):

    return {
        "indice": indice,
        "mejor_modelo": {
            "con_futuro": seleccionar_mejor_modelo(resultados, "con_futuro"),
            "sin_futuro": seleccionar_mejor_modelo(resultados, "sin_futuro"),
        }
    }
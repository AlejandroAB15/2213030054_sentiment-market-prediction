from pathlib import Path
import pandas as pd
from prediccion.buildDF_bursatil import build_dataset_prediccion
from prediccion.evaluacion_modelos import (
    evaluar_modelos_por_indice,
    construir_resumen_indice
)
from prediccion.construir_DF_resultados import construir_dataset_resultados


def run_prediccion():

    dataset_path = Path("data/resultados/dataset_clasificado.json")
    raws_path = Path("data/raws")

    df_final = build_dataset_prediccion(
        dataset_path=dataset_path,
        raws_path=raws_path
    )

    # Para eliminar filas con errores a la hora de clasificar
    df_model_valido = df_final[
        df_final["sentimiento_label"].isin(["POS", "NEG", "NEU"])
    ].copy()

    indices = ["dji", "nasdaq", "sp500"]

    resultados_modelos = {}
    resumen_indices = []

    output_file = "data/resultados/comparativo_indices.xlsx"

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

        for indice in indices:

            resultado = evaluar_modelos_por_indice(
                df_model_valido,
                indice
            )

            df_general = resultado["general_con_futuro"]["df"]
            df_especifico = resultado["especifico_con_futuro"]["df"]

            df_resultados = construir_dataset_resultados(
                df_general,
                df_especifico,
                indice
            )

            resultados_modelos[indice] = df_resultados

            df_resultados.tail(90).to_excel(
                writer,
                sheet_name=indice,
                index=False
            )

            resumen = construir_resumen_indice(indice, resultado)
            resumen_indices.append(resumen)

    return df_final, resultados_modelos, resumen_indices
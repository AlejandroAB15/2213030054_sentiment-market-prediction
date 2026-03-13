from pathlib import Path
import pandas as pd
from prediccion.buildDF_bursatil import build_dataset_prediccion
from prediccion.evaluacion_modelos import ( evaluar_modelos_indice, construir_tabla_comparativa )

def run_prediccion():

    dataset_path = Path("data/resultados/dataset_clasificado.json")
    raws_path = Path("data/raws")

    df_final = build_dataset_prediccion(
        dataset_path=dataset_path,
        raws_path=raws_path
    )

    # Para eliminar registros clasificados con error
    df_model_valido = df_final[df_final["sentimiento_label"].isin(["POS","NEG","NEU"])].copy()

    indices = ["dji", "nasdaq", "sp500"]

    output_file = "data/resultados/comparativo_indices.xlsx"

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

        for indice in indices:

            resultado = evaluar_modelos_indice(
                df_model_valido,
                indice
            )

            tabla_futuro = construir_tabla_comparativa(
                resultado["general_con_futuro"],
                resultado["especifico_con_futuro"],
                indice
            )

            tabla_futuro.to_excel(
                writer,
                sheet_name=f"{indice}_con_futuro",
                index=False
            )

            tabla_sin_futuro = construir_tabla_comparativa(
                resultado["general_sin_futuro"],
                resultado["especifico_sin_futuro"],
                indice
            )

            tabla_sin_futuro.to_excel(
                writer,
                sheet_name=f"{indice}_sin_futuro",
                index=False
            )

    return df_final
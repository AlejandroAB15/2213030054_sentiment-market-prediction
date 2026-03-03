from pathlib import Path
from utils.logger import get_logger
from utils.logging_utils import log_and_print
from prediccion.buildDF_bursatil import build_dataset_prediccion
from prediccion.componentes_modelo import calcular_componentes_modelo
import pandas as pd
from persistencia.mongo_uploader import upload_to_mongo

def run_prediccion():
    
    UPLOAD_TO_MONGO = True

    dataset_path = Path("data/resultados/dataset_clasificado.json")
    raws_path = Path("data/raws")

    df_final = build_dataset_prediccion(
        dataset_path=dataset_path,
        raws_path=raws_path
    )
    
    df_model_valido = df_final[
        df_final["sentimiento_label"].isin(["POS", "NEG", "NEU"])
    ].copy()

    df_sp500_semana = calcular_componentes_modelo(
        df=df_model_valido,
        indice_base="sp500",
        fecha_inicio="2025-03-10",
        n_dias=7,
        ventana=7
    )

    df_nasdaq_mes = calcular_componentes_modelo(
        df=df_model_valido,
        indice_base="nasdaq",
        fecha_inicio="2025-06-01",
        n_dias=30,
        ventana=30
    )

    df_dji_trimestre = calcular_componentes_modelo(
        df=df_model_valido,
        indice_base="dji",
        fecha_inicio="2025-02-01",
        n_dias=90,
        ventana=90
    )

    # with pd.ExcelWriter("data/resultados/modelo_componentes.xlsx",
    #                 engine="openpyxl") as writer:
    #     df_sp500_semana.to_excel(writer, sheet_name="sp500_semana", index=False)
    #     df_nasdaq_mes.to_excel(writer, sheet_name="nasdaq_mes", index=False)
    #     df_dji_trimestre.to_excel(writer, sheet_name="dji_trimestre", index=False)

    return df_final

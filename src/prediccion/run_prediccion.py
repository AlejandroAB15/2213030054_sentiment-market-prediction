from pathlib import Path
from utils.logger import get_logger
from utils.logging_utils import log_and_print
from prediccion.buildDF_bursatil import build_dataset_prediccion
from prediccion.componentes_modelo import calcular_componentes_modelo

def run_prediccion():

    logger = get_logger(__name__)

    dataset_path = Path("data/resultados/dataset_clasificado.json")
    raws_path = Path("data/raws")

    df_final = build_dataset_prediccion(
        dataset_path=dataset_path,
        raws_path=raws_path
    )

    df_sp500_semana = calcular_componentes_modelo(
        df=df_final,
        indice_base="sp500",
        fecha_inicio="2025-03-10",
        n_dias=7,
        ventana=7
    )

    print(df_sp500_semana.head(10))
    print(df_sp500_semana[["T1", "T2", "T3"]].describe())

    df_nasdaq_mes = calcular_componentes_modelo(
        df=df_final,
        indice_base="nasdaq",
        fecha_inicio="2025-06-01",
        n_dias=30,
        ventana=30
    )

    df_dji_trimestre = calcular_componentes_modelo(
        df=df_final,
        indice_base="dji",
        fecha_inicio="2025-02-01",
        n_dias=90,
        ventana=90
    )


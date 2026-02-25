from pathlib import Path
from utils.logger import get_logger
from utils.logging_utils import log_and_print
from prediccion.buildDF_bursatil import build_dataset_prediccion
from prediccion.componentes_modelo import calcular_terminos_unicos

def run_prediccion():

    logger = get_logger(__name__)

    dataset_path = Path("data/resultados/dataset_clasificado.json")
    raws_path = Path("data/raws")

    df_final = build_dataset_prediccion(
        dataset_path=dataset_path,
        raws_path=raws_path
    )

    df_sp500 = calcular_terminos_unicos(
        df=df_final,
        indice_base="sp500",
        fecha_inicio="2025-03-10",
        n_dias=7
    )

    df_nasdaq = calcular_terminos_unicos(
        df=df_final,
        indice_base="nasdaq",
        fecha_inicio="2025-06-01",
        n_dias=30
    )

    df_dji = calcular_terminos_unicos(
        df=df_final,
        indice_base="dji",
        fecha_inicio="2025-02-01",
        n_dias=90
    )

    print(df_sp500)
    print(df_nasdaq)
    print(df_dji)

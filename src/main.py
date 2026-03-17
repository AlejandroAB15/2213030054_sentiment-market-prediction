from pathlib import Path
from utils.logger import setup_logger

from adquisicion.run_adquisicion import run_adquisicion
from preprocesado.run_preprocesado import run_preprocesado
from clasificacion.run_clasificacion import run_clasificacion
from prediccion.run_prediccion import run_prediccion
from persistencia.mongo_uploader import (
    upload_preprocesado,
    upload_to_mongo,
    upload_predicciones,
    upload_resumen_modelos
)

UPLOAD_TO_MONGO = True

def main():

    setup_logger()

    # run_adquisicion()

    # _, _, resumen_preprocesado = run_preprocesado()

    # run_clasificacion()

    df_final, resultados_modelos, resumen_indices = run_prediccion()

    if UPLOAD_TO_MONGO:

        # upload_preprocesado(resumen_preprocesado)

        # upload_to_mongo(
        #     df_final=df_final,
        #     ruta_relevantes=Path("data/procesados/dataset_relevante.json"),
        #     ruta_no_relevantes=Path("data/procesados/dataset_no_relevante.json"),
        # )

        for indice, df_modelo in resultados_modelos.items():
            upload_predicciones(df_modelo, indice)

        upload_resumen_modelos(resumen_indices)


if __name__ == "__main__":
    main()
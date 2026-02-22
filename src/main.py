from utils.logger import setup_logger
from adquisicion.run_adquisicion import run_adquisicion
from preprocesado.run_preprocesado import run_preprocesado
from clasificacion.run_clasificacion import run_clasificacion
from prediccion.run_prediccion import run_prediccion


def main():
    setup_logger()

    #run_adquisicion()
    #run_preprocesado()
    run_clasificacion()
    #run_prediccion()


if __name__ == "__main__":
    main()

from utils.logger import setup_logger
from adquisicion.run_adquisicion import run_adquisicion
from preprocesado.run_preprocesado import run_preprocesado


def main():
    setup_logger()

    #run_adquisicion()
    run_preprocesado()


if __name__ == "__main__":
    main()

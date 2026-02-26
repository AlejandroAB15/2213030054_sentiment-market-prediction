import json
import requests
import re
import pandas as pd
from pathlib import Path

MODEL = "llama3.1:8b"


def clasificar_llama(texto: str):

    prompt = f"""
    Eres un analista económico cuantitativo.

    Tu tarea es clasificar el impacto económico esperado sobre mercados financieros
    derivado EXCLUSIVAMENTE de acciones, políticas, decisiones regulatorias,
    fiscales o comerciales descritas en el texto.

    Procedimiento obligatorio:

    1. Identifica si existe una acción económica concreta (ej. aranceles, impuestos,
    regulaciones, acuerdos comerciales, sanciones, estímulos).
    2. Si NO existe una acción económica concreta → NEU.
    3. Si existe acción económica que aumenta costos, riesgo o incertidumbre → NEG.
    4. Si existe acción económica que reduce costos, riesgo o estimula actividad → POS.
    5. No clasifiques como NEG solo por polémica, escándalo o tono negativo.
    6. Ignora lenguaje editorial, satírico u ofensivo.

    Responde únicamente en formato JSON:
    {{"label": "POS"}}

    Texto:
    {texto}
    """

    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": MODEL,
            "prompt": prompt,
            "temperature": 0,
            "stream": False
        }
    )

    raw = response.json()["response"].strip()

    # Extraer JSON aunque venga con texto adicional
    match = re.search(r'\{.*?\}', raw, re.DOTALL)

    if match:
        json_str = match.group(0)
        try:
            label = json.loads(json_str)["label"]
        except:
            label = "ERROR"
    else:
        label = "ERROR"

    return label, raw


def run_piloto():

    print("\n" + "="*80)
    print("INICIO PILOTO LLAMA 3.1 8B")
    print("="*80 + "\n")

    input_path = Path("data/piloto_trump_impacto.json")
    output_path = Path("data/piloto_trump_resultados.json")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Total noticias a evaluar: {len(data)}\n")

    for i, registro in enumerate(data, start=1):

        print("-"*60)
        print(f"Procesando noticia {i} de {len(data)} (ID: {registro['id']})")
        print("-"*60)

        texto = registro["contenido"]

        etiqueta_modelo, respuesta_cruda = clasificar_llama(texto)

        registro["etiqueta_modelo"] = etiqueta_modelo
        registro["respuesta_modelo"] = respuesta_cruda

        print(f"Etiqueta modelo: {etiqueta_modelo}\n")

    # Guardar resultados
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("\n" + "="*80)
    print("RESULTADOS GUARDADOS")
    print("="*80 + "\n")

    analizar_resultados(output_path)


def analizar_resultados(path):

    print("="*80)
    print("ANÁLISIS DE RESULTADOS")
    print("="*80)

    df = pd.read_json(path)

    # Mapear etiquetas manuales
    map_manual = {
        "POSITIVO": "POS",
        "NEGATIVO": "NEG",
        "NEUTRO": "NEU"
    }

    df["etiqueta_manual_mapeada"] = df["etiqueta"].map(map_manual)

    df["acierto"] = (
        df["etiqueta_manual_mapeada"] == df["etiqueta_modelo"]
    ).astype(int)

    accuracy = df["acierto"].mean()

    print(f"\nAccuracy: {round(accuracy, 4)}\n")

    print("Distribución manual:")
    print(df["etiqueta_manual_mapeada"].value_counts(), "\n")

    print("Distribución modelo:")
    print(df["etiqueta_modelo"].value_counts(), "\n")

    print("Total ERROR:", (df["etiqueta_modelo"] == "ERROR").sum(), "\n")

    # ==========================
    # MATRIZ DE CONFUSIÓN
    # ==========================

    print("="*80)
    print("MATRIZ DE CONFUSIÓN")
    print("="*80)

    confusion = pd.crosstab(
        df["etiqueta_manual_mapeada"],
        df["etiqueta_modelo"],
        rownames=["Manual"],
        colnames=["Modelo"]
    )

    print(confusion, "\n")

    # ==========================
    # ERRORES DETALLADOS
    # ==========================

    print("="*80)
    print("ERRORES DETECTADOS")
    print("="*80)

    errores = df[df["acierto"] == 0]

    if len(errores) == 0:
        print("No hay errores.")
    else:
        for _, row in errores.iterrows():
            print("-"*60)
            print(f"ID: {row['id']}")
            print(f"Manual: {row['etiqueta_manual_mapeada']}")
            print(f"Modelo: {row['etiqueta_modelo']}")
            print("-"*60)
            print("Respuesta cruda del modelo:")
            print(row["respuesta_modelo"])
            print("\n")


if __name__ == "__main__":
    run_piloto()
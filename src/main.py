import json
import requests
import re
import pandas as pd
from pathlib import Path
from pysentimiento import create_analyzer
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix

MODEL = "llama3.1:8b"

def clasificar_llama(texto: str):

    prompt = f"""
Eres un analista económico.

Analiza el siguiente texto periodístico y clasifica el impacto económico esperado 
sobre mercados financieros derivado de las acciones, políticas o decisiones descritas.

Criterios:

- Si las acciones descritas aumentan riesgo económico, tensiones comerciales, 
  incertidumbre regulatoria o costos → NEG
- Si las acciones descritas estimulan inversión, reducen riesgo o favorecen 
  actividad económica → POS
- Si el texto es principalmente editorial, satírico, opinativo o no describe 
  una acción económica concreta → NEU
- No inferir impacto económico únicamente por tono negativo o polémico
- Ignorar lenguaje figurativo u ofensivo

No evalúes ideología ni postura política.
No agregues explicación.

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

    match = re.search(r'\{.*?\}', raw, re.DOTALL)

    if match:
        json_str = match.group(0)
        try:
            return json.loads(json_str)["label"]
        except:
            return "ERROR"
    else:
        return "ERROR"


# =====================================================
# ROBERTUITO
# =====================================================
def clasificar_robertuito(analyzer, texto: str):
    result = analyzer.predict(texto)
    return result.output


# =====================================================
# MAIN
# =====================================================
def run_comparativo():

    print("\n" + "="*80)
    print("COMPARATIVO FINAL: LLAMA vs ROBERTUITO")
    print("="*80 + "\n")

    analyzer = create_analyzer(task="sentiment", lang="es")

    input_path = Path("data/piloto_trump_impacto.json")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    manual_labels = []
    llama_labels = []
    robertuito_labels = []

    map_manual = {
        "POSITIVO": "POS",
        "NEGATIVO": "NEG",
        "NEUTRO": "NEU"
    }

    for i, registro in enumerate(data, start=1):

        print(f"Procesando {i}/{len(data)}")

        texto = registro["contenido"]

        manual = map_manual[registro["etiqueta"]]
        llama = clasificar_llama(texto)
        robertuito = clasificar_robertuito(analyzer, texto)

        manual_labels.append(manual)
        llama_labels.append(llama)
        robertuito_labels.append(robertuito)

    # ==========================
    # MÉTRICAS
    # ==========================

    print("\n" + "="*80)
    print("RESULTADOS")
    print("="*80)

    # Accuracy
    acc_llama = accuracy_score(manual_labels, llama_labels)
    acc_robertuito = accuracy_score(manual_labels, robertuito_labels)

    print(f"\nAccuracy Llama: {round(acc_llama,4)}")
    print(f"Accuracy Robertuito: {round(acc_robertuito,4)}")

    # Classification report
    print("\n" + "="*80)
    print("REPORTE DETALLADO LLAMA")
    print("="*80)
    print(classification_report(manual_labels, llama_labels))

    print("\n" + "="*80)
    print("REPORTE DETALLADO ROBERTUITO")
    print("="*80)
    print(classification_report(manual_labels, robertuito_labels))

    # Matriz de confusión
    print("\n" + "="*80)
    print("MATRIZ DE CONFUSIÓN LLAMA")
    print("="*80)
    print(confusion_matrix(manual_labels, llama_labels))

    print("\n" + "="*80)
    print("MATRIZ DE CONFUSIÓN ROBERTUITO")
    print("="*80)
    print(confusion_matrix(manual_labels, robertuito_labels))


if __name__ == "__main__":
    run_comparativo()
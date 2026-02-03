# import json

# from adquisicion.fuentes.el_universal import fetch_base as fetch_universal
# from adquisicion.fuentes.el_pais import fetch_base as fetch_pais


# def main():
#     noticias_universal = fetch_universal()
#     noticias_pais = fetch_pais()

#     with open("el_universal_base.json", "w", encoding="utf-8") as f:
#         json.dump(noticias_universal, f, ensure_ascii=False, indent=2)

#     with open("el_pais_base.json", "w", encoding="utf-8") as f:
#         json.dump(noticias_pais, f, ensure_ascii=False, indent=2)


# if __name__ == "__main__":
#     main()

# from adquisicion.fuentes import indices

# if __name__ == "__main__":
#     start_date = "2025-01-01"
#     end_date = "2026-01-31"

#     print("Descargando índices bursátiles...")
#     datos_indices = indices.descargar_indices(start_date, end_date, guardar_csv=True)

#     for nombre, df in datos_indices.items():
#         print(f"{nombre}: {len(df)} registros descargados y guardados en CSV")

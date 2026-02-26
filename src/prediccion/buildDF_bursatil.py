from pathlib import Path
import pandas as pd

def _load_index(path: Path, prefix: str) -> pd.DataFrame:
    df = pd.read_csv(path, skiprows=[1])
    df.columns = df.columns.str.strip()

    # Se elimina la fila residual que contiene la cadena "Date"
    df = df[df["Price"] != "Date"]

    df["Price"] = pd.to_datetime(df["Price"])
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")

    df = df.sort_values("Price")

    df = df[["Price", "Close"]].rename(columns={
        "Price": "fecha",
        "Close": f"close_{prefix}"
    })

    return df.reset_index(drop=True)


def _merge_index(
    df_base: pd.DataFrame,
    df_index: pd.DataFrame,
    prefix: str
) -> pd.DataFrame:
    
    # merge_asof busca la fila más cercana en el tiempo según una dirección.

    df_index_sorted = df_index.sort_values("fecha")

    # Para cada fila del df clasificado se busca en el df de indices la fila asociada y une sus columnas
    df_out = pd.merge_asof(
        df_base.sort_values("fecha_t"),
        df_index_sorted,
        left_on="fecha_t",
        right_on="fecha",
        direction="forward"  # primer día hábil >= fecha noticia
    )

    # Al fusionar queda una columna extra de fecha, se desecha y se renombra la restante
    df_out = df_out.rename(columns={
        f"close_{prefix}": f"close_{prefix}_t"
    }).drop(columns=["fecha_y"]).rename(columns={"fecha_x": "fecha"})

    df_out = pd.merge_asof(
        df_out.sort_values("fecha_t7"),
        df_index_sorted,
        left_on="fecha_t7",
        right_on="fecha",
        direction="forward"
    )

    df_out = df_out.rename(columns={
        f"close_{prefix}": f"close_{prefix}_t7"
    }).drop(columns=["fecha_y"]).rename(columns={"fecha_x": "fecha"})

    return df_out

from pathlib import Path
import pandas as pd


def build_dataset_prediccion(
    dataset_path: Path,
    raws_path: Path
) -> pd.DataFrame:

    df_news = pd.read_json(dataset_path)

    df_news["fecha"] = pd.to_datetime(
        df_news["fecha"],
        format="%d-%m-%Y"
    )

    df_news = df_news.sort_values("fecha").reset_index(drop=True)

    df_model = df_news.copy()

    df_model["fecha_t"] = df_model["fecha"]
    df_model["fecha_t7"] = df_model["fecha"] + pd.Timedelta(days=7)

    df_sp500 = _load_index(raws_path / "SP500_historico.csv", "sp500")
    df_nasdaq = _load_index(raws_path / "NASDAQ_historico.csv", "nasdaq")
    df_dji = _load_index(raws_path / "DJI_historico.csv", "dji")

    df_model = _merge_index(df_model, df_sp500, "sp500")
    df_model = _merge_index(df_model, df_nasdaq, "nasdaq")
    df_model = _merge_index(df_model, df_dji, "dji")

    final_columns = [
        "titulo",
        "subtitulo",
        "fecha",
        "fuente",
        "sentimiento_label",
        "close_sp500_t",
        "close_sp500_t7",
        "close_nasdaq_t",
        "close_nasdaq_t7",
        "close_dji_t",
        "close_dji_t7",
    ]

    df_final = (
        df_model[final_columns]
        .sort_values("fecha")
        .reset_index(drop=True)
    )

    return df_final

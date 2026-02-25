import pandas as pd

def calcular_terminos_unicos(
    df: pd.DataFrame,
    indice_base: str,
    fecha_inicio: str,
    n_dias: int,
) -> pd.DataFrame:
    
    """
    Calcula T1, T2 y T3 una única vez sobre un intervalo completo
    de n_dias, devolviendo solo los términos finales.
    """

    df = df.copy()

    close_t_col = f"close_{indice_base}_t"
    close_t7_col = f"close_{indice_base}_t7"

    df["fecha"] = pd.to_datetime(df["fecha"])
    fecha_inicio = pd.to_datetime(fecha_inicio)

    fecha_fin = fecha_inicio + pd.Timedelta(days=n_dias)

    df_intervalo = df[
        (df["fecha"] >= fecha_inicio) &
        (df["fecha"] < fecha_fin)
    ].copy()

    if df_intervalo.empty:
        raise ValueError("El intervalo seleccionado no contiene datos.")

    # Sentimiento numérico
    mapping = {"POS": 1, "NEG": -1, "NEU": 0}
    df_intervalo["s_i"] = df_intervalo["sentimiento_label"].map(mapping).fillna(0)

    epsilon = 1e-8

    # --- T1 ---
    promedio_precio = df_intervalo[close_t_col].mean()
    max_precio = df_intervalo[close_t_col].max()
    T1 = promedio_precio / (max_precio + epsilon)

    # --- T2 ---
    promedio_sentimiento = df_intervalo["s_i"].mean()
    rango_sentimiento = df_intervalo["s_i"].max() - df_intervalo["s_i"].min()
    rango_sentimiento = rango_sentimiento if rango_sentimiento != 0 else epsilon
    T2 = promedio_sentimiento / rango_sentimiento

    # --- T3 ---
    desviacion_precio = df_intervalo[close_t_col].std()
    desviacion_precio = desviacion_precio if desviacion_precio != 0 else epsilon
    T3 = promedio_precio / desviacion_precio

    # Fecha final del bloque
    fecha_final = df_intervalo["fecha"].max()

    resultado = pd.DataFrame({
        "fecha": [fecha_final],
        close_t_col: [df_intervalo[close_t_col].iloc[-1]],
        close_t7_col: [df_intervalo[close_t7_col].iloc[-1]],
        "T1": [T1],
        "T2": [T2],
        "T3": [T3],
    })

    return resultado
import pandas as pd

def calcular_componentes_modelo(
    df: pd.DataFrame,
    indice_base: str,
    fecha_inicio: str,
    n_dias: int,
    ventana: int | None = None,
) -> pd.DataFrame:

    df = df.copy()

    close_t_col = f"close_{indice_base}_t"
    close_t7_col = f"close_{indice_base}_t7"

    if close_t_col not in df.columns or close_t7_col not in df.columns:
        raise ValueError(f"Columnas para índice '{indice_base}' no encontradas.")

    df["fecha"] = pd.to_datetime(df["fecha"])
    fecha_inicio = pd.to_datetime(fecha_inicio)

    fecha_min = df["fecha"].min()
    fecha_max = df["fecha"].max()

    if fecha_inicio < fecha_min or fecha_inicio > fecha_max:
        raise ValueError(
            f"La fecha_inicio está fuera del rango disponible "
            f"({fecha_min.date()} - {fecha_max.date()})."
        )

    fecha_fin = fecha_inicio + pd.Timedelta(days=n_dias)

    if fecha_fin > fecha_max:
        raise ValueError(
            f"El intervalo solicitado excede el rango disponible. "
            f"Fecha máxima permitida: {fecha_max.date()}."
        )

    df_intervalo = df[
        (df["fecha"] >= fecha_inicio) &
        (df["fecha"] < fecha_fin)
    ].copy()

    ventana_default = max(n_dias, 1)

    if ventana is None:
        ventana = ventana_default

    mapping = {"POS": 1, "NEG": -1, "NEU": 0}
    df_intervalo["s_i"] = df_intervalo["sentimiento_label"].map(mapping).fillna(0)

    epsilon = 1e-8

    # 1er término - Componente cuantitativa

    y_i = df_intervalo[close_t_col].rolling(
        window=ventana,
        min_periods=1
    ).mean()

    max_historico = df_intervalo[close_t_col].max()

    T1 = y_i / (max_historico + epsilon)

    # 2do término - Componente cualitativa

    polaridad_i = df_intervalo["s_i"].rolling(
        window=ventana,
        min_periods=1
    ).mean()

    distancia = df_intervalo["s_i"].max() - df_intervalo["s_i"].min()
    distancia = distancia if distancia != 0 else epsilon

    T2 = polaridad_i / distancia

    # 3er término - Componente estadística

    """ 
    promedio_i = df_intervalo[close_t_col].rolling(
        window=ventana,
        min_periods=1
    ).mean()

    desviacion_i = df_intervalo[close_t_col].rolling(
        window=ventana,
        min_periods=1
    ).std()

    desviacion_i = desviacion_i.fillna(0)

    T3 = promedio_i / (desviacion_i + epsilon) 
    """
    promedio_i = df_intervalo[close_t_col].rolling(
        window=ventana,
        min_periods=ventana
    ).mean()

    desviacion_i = df_intervalo[close_t_col].rolling(
        window=ventana,
        min_periods=ventana
    ).std()

    T3 = promedio_i / (desviacion_i + 1e-8)

    # Resultado
    resultado = pd.DataFrame({
        "fecha": df_intervalo["fecha"],
        close_t_col: df_intervalo[close_t_col],
        close_t7_col: df_intervalo[close_t7_col],
        "T1": T1,
        "T2": T2,
        "T3": T3,
    })

    return resultado
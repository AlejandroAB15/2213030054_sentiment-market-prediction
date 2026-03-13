import pandas as pd
import numpy as np

def construir_matriz_X(df: pd.DataFrame, columnas: list[str]) -> np.ndarray:
    
    # Valores de las medias moviles
    X = df[columnas].to_numpy(dtype=float)

    unos = np.ones((X.shape[0],1))

    # Se pega la columna de unos para el β0
    X = np.hstack([unos, X])

    return X

def construir_vector_y(df: pd.DataFrame, columna_objetivo: str) -> np.ndarray: 

    y = df[columna_objetivo].to_numpy(dtype=float)

    return y.reshape(-1,1)

def calcular_beta(X: np.ndarray, y: np.ndarray) -> np.ndarray:

    # Transpuesta de la matriz X
    Xt = X.T

    # Multiplicacion de X por su transpuesta (matriz cuadrada)
    XtX = Xt @ X

    # Matriz inversa de la matriz cuadrada
    XtX_inv = np.linalg.pinv(XtX)

    # Matriz transpuesta por columna objetivo
    XtY = Xt @ y

    beta = XtX_inv @ XtY

    return beta

def predecir(X: np.ndarray, beta: np.ndarray) -> np.ndarray:

    y_hat = X @ beta

    return y_hat
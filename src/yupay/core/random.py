import polars as pl
import random


class Randomizer:
    """
    Utilidades de aleatoriedad basadas en Python standard library y Polars.
    Garantiza reproducibilidad mediante semillas (en la medida de lo posible con random).
    """

    def __init__(self, seed: int = 42):
        self.seed = seed
        random.seed(seed)

    def sample_from_list(self, items: list, n: int, weights: list = None) -> pl.Series:
        """
        Muestreo aleatorio de una lista de items con pesos opcionales.
        """
        # random.choices realiza muestreo con reemplazo
        sampled = random.choices(items, weights=weights, k=n)
        return pl.Series(sampled)

    def add_noise(self, series: pl.Series, null_rate: float = 0.0) -> pl.Series:
        """
        Introduce valores nulos de forma controlada.
        """
        if null_rate <= 0:
            return series

        # Generar máscara booleana usando lista de comprensión (puede ser lento en muy alta escala)
        # pero elimina dependencia de numpy.
        length = len(series)
        mask = [random.random() < null_rate for _ in range(length)]

        # Convertir a Series para usar en Polars
        mask_series = pl.Series(mask)

        # Usar when/then de Polars si fuera expresión, pero aquí manipulamos Series física
        # Opción eficiente: zip_with o set_at_idx
        return series.zip_with(
            mask_series,
            pl.Series([None] * length, dtype=series.dtype)
        )

    # Métodos helper para reemplazar acceso directo a rng
    def choice(self, items: list):
        return random.choice(items)

    def random(self) -> float:
        return random.random()

    def random_index_expr(self, n_options: int, col_name: str = "index") -> pl.Expr:
        """
        Genera una expresión de Polars que produce un índice aleatorio entre 0 y n_options - 1.
        Utiliza hashing determinista basado en una columna de entrada (por defecto 'index').
        Es 'Lazy' y no materializa memoria.
        """
        # A simple pseudo-random generator using hash and modulo.
        # We add a salt based on self.seed to vary the results.
        if n_options <= 0:
            raise ValueError("n_options must be positive")

        return (
            pl.col(col_name)
            .hash(seed=self.seed)  # Use the seed for reproducibility
            .mod(n_options)
            .cast(pl.UInt32)
        )

import yaml
import pathlib
from typing import Any


class Settings:
    """
    Gestor de configuración centralizado.
    Maneja la carga de YAMLs dinámicamente y permite overrides.
    """

    def __init__(self, config_dir: str = None):
        if config_dir is None:
            # Por defecto busca en la raíz del proyecto (asumiendo que se ejecuta desde ahí)
            # o un nivel arriba si se instala como paquete editable y se ejecuta script.
            
            # Opción robusta: Path.cwd() / "config" si asumimos ejecución desde root
            self.config_dir = pathlib.Path.cwd() / "config"
        else:
            self.config_dir = pathlib.Path(config_dir)

    def load_defaults(self) -> dict[str, Any]:
        path = self.config_dir / "defaults.yaml"
        return self._read_yaml(path)

    def load_locale(self, locale: str) -> dict[str, Any]:
        """
        Carga la configuración de localización (nombres, regiones, etc.)
        """
        locale_path = self.config_dir / "locales" / locale
        if not locale_path.exists():
            # Fallback simple
            return {}

        config = {}
        for file in locale_path.glob("*.yaml"):
            data = self._read_yaml(file)
            config[file.stem] = data
        return config

    def load_domain(self, domain_name: str) -> dict[str, Any]:
        domain_path = self.config_dir / "domains" / domain_name

        if not domain_path.exists():
            raise FileNotFoundError(
                f"Configuración para el dominio '{domain_name}' no encontrada en {domain_path}")

        # Si es un archivo directo (legacy support o config simple)
        if domain_path.is_file():
            return self._read_yaml(domain_path)

        # Si es un directorio, cargar todo recursivamente
        config = {}
        for file in domain_path.rglob("*.yaml"):
            # Estructura del dict basada en la ruta relativa
            # ej: catalogs/products.yaml -> config['catalogs']['products']
            rel_path = file.relative_to(domain_path)
            data = self._read_yaml(file)

            # Si es main.yaml, mezclar en la raíz
            if rel_path.name == "main.yaml" and len(rel_path.parts) == 1:
                config = self.merge_configs(config, data)
            else:
                # Construir anidamiento
                current = config
                for part in rel_path.parts[:-1]:
                    current = current.setdefault(part, {})
                current[rel_path.stem] = data

        return config

    def _read_yaml(self, path: pathlib.Path) -> dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    @staticmethod
    def merge_configs(base: dict, override: dict) -> dict:
        """
        Mezcla recursiva de diccionarios de configuración.
        """
        result = base.copy()
        for key, value in override.items():
            if isinstance(value, dict) and key in result and isinstance(result[key], dict):
                result[key] = Settings.merge_configs(result[key], value)
            else:
                result[key] = value
        return result

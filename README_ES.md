# Yupay: Generador Realista de Datos ERP Sintéticos

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-stable-brightgreen)

Yupay es un generador de datos sintéticos de alto rendimiento diseñado para simular escenarios ERP realistas. Genera datos sucios, simula patrones temporales (estacionalidad, tendencias) y gestiona el uso de recursos de manera eficiente.

## Documentación

- **[Manual de Usuario](docs/USER_MANUAL.md)** / **[Español](docs/USER_MANUAL_ES.md)**: Guía de configuración e instrucciones de uso.
- **[Arquitectura](docs/ARCHITECTURE.md)**: Profundización en MemoryGuard, Entropy Engine y Registry.
- **[Contribución](docs/CONTRIBUTING.md)**: Configuración de desarrollo y cómo agregar nuevos dominios.
- **[Changelog](CHANGELOG.md)**: Historial de versiones.

## Características Clave

- **Generación Guiada por Tiempo**: Simula años de datos con volúmenes diarios distribuidos por Poisson.
- **Motor de Datos Sucios**: Inyecta nulos, duplicados, FKs rotas y ruido en cadenas.
- **Seguridad de Memoria**: `MemoryGuard` previene caídas por OOM con umbrales escalonados.
- **Batching Proactivo**: Cambia automáticamente de modo Monolítico a Lotes para grandes conjuntos de datos.

## Instalación

```bash
# Clonar el repositorio
git clone https://github.com/manuelvasquezab/yupay.git
cd yupay

# Instalar dependencias (Python 3.10+)
pip install -e .
```

## Licencia

MIT

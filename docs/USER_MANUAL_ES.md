# Manual de Usuario de Yupay

Bienvenido al manual de usuario de **Yupay**. Esta guía se centra en cómo configurar y ejecutar el generador para producir los datos que necesites.

## 1. Inicio Rápido

Ejecuta la generación para el dominio de Ventas (Sales):

```bash
yupay generate sales
```

Esto utiliza la configuración predeterminada en `config/defaults.yaml` (valores técnicos por defecto) y `config/main.yaml` (anulaciones del usuario).

## 2. Configuración (`config/main.yaml`)

El archivo `config/main.yaml` es tu centro de control.

### 2.1 Ventana de Simulación
Define las fechas de inicio y fin. El motor maneja años bisiestos y la distribución diaria de volumen automáticamente.

```yaml
start_date: "2010-01-01"
end_date: "2024-12-31"
```

### 2.2 Control de Volumen
`daily_avg_transactions` controla el "ritmo cardíaco" de la simulación. Se aplica una distribución de Poisson a esta media para crear una varianza natural.

```yaml
daily_avg_transactions: 15000 
```

### 2.3 Niveles de Caos (Datos Sucios)
Puedes elegir qué tan "sucios" deben ser los datos.

- `low`: Casi limpios. Nulos ocasionales.
- `medium`: Errores realistas. Algunos duplicados, algunos nulos, claves rotas raras.
- `high`: Hostil. Muchos duplicados, claves rotas, paradojas temporales (Fecha Pedido > Fecha Envío).

```yaml
chaos_level: "medium"
chaos:
  global_seed: 42  # Cambia esto para obtener un resultado aleatorio diferente
```

## 3. Gestión de Salida
Los datos se guardan por defecto en `data/[domain]/data_[timestamp]`.

Para ver lo que has generado:
```bash
yupay tools list
```

Para limpiar ejecuciones antiguas:
```bash
yupay tools clear --domain sales
```

## 4. Seguridad de Recursos (MemoryGuard)
Yupay protege tu PC.
- **Monitoreo**: Observa el uso de tu RAM en tiempo real.
- **Throttling (Freno)**: Si el uso de RAM > 80%, ralentiza la generación y reduce el tamaño de los lotes.
- **Abortar**: Si el uso de RAM > 90%, se detiene para evitar un bloqueo del sistema.

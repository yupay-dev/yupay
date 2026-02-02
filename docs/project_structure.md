# |Project Structure

```text
Yupay
├── .gitignore
├── config/
│   ├── __init__.py
│   ├── defaults.yaml
│   ├── distributions.yaml
│   ├── domains/
│   │   ├── finance/
│   │   ├── finance.yaml
│   │   ├── hr/
│   │   ├── hr.yaml
│   │   └── sales/
│   │       ├── catalogs/
│   │       │   └── products.yaml
│   │       └── main.yaml
│   ├── locales/
│   │   └── es_PE/
│   │       └── names.yaml
│   └── locales.yaml
├── data/
├── docs/
|   ├── project_structure.md
├── LICENSE
├── main.py
├── pyproject.toml
├── README.md
├── requirements.txt
└── yupay/
    ├── __init__.py
    ├── cli.py
    ├── core/
    │   ├── __init__.py
    │   ├── dataset.py
    │   ├── filesystem.py
    │   ├── generator.py
    │   ├── random.py
    │   ├── settings.py
    │   ├── sink.py
    │   ├── system.py
    │   └── time.py
    ├── domains/
    │   ├── __init__.py
    │   ├── finance/
    │   ├── hr/
    │   └── sales/
    │       ├── __init__.py
    │       ├── customers.py
    │       ├── orders.py
    │       ├── payments.py
    │       └── products.py
    ├── sinks/
    │   ├── __init__.py
    │   └── definitions.py
    └── utils/
```

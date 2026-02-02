# |Project Structure

```text
Yupay
├── config/
│   ├── domains/
│   │   ├── finance/
│   │   │   └── finance.yaml
│   │   ├── hr/
│   │   │   └── hr.yaml
│   │   └── sales/
│   │       ├── catalogs/
│   │       │   └── products.yaml
│   │       └── main.yaml
│   ├── locales/
│   │   └── messages.pot
│   ├── defaults.yaml
│   ├── distributions.yaml
│   ├── main.yaml
│   └── __init__.py
├── docs/
│   ├── ARCHITECTURE.md
│   ├── CONTRIBUTING.md
│   ├── PROJECT_STRUCTURE.md
│   ├── USER_MANUAL.md
│   └── USER_MANUAL_ES.md
├── src/
│   └── yupay/
│       ├── core/
│       │   ├── __init__.py
│       │   ├── dataset.py
│       │   ├── entropy.py
│       │   ├── estimator.py
│       │   ├── filesystem.py
│       │   ├── generator.py
│       │   ├── i18n.py
│       │   ├── memory.py
│       │   ├── random.py
│       │   ├── registry.py
│       │   ├── settings.py
│       │   ├── sink.py
│       │   ├── system.py
│       │   └── time.py
│       ├── domains/
│       │   ├── finance/
│       │   ├── hr/
│       │   ├── sales/
│       │   │   ├── catalogs/
│       │   │   ├── __init__.py
│       │   │   ├── customers.py
│       │   │   ├── handler.py
│       │   │   ├── orders.py
│       │   │   ├── payments.py
│       │   │   └── products.py
│       │   └── __init__.py
│       ├── locales/
│       │   ├── en/
│       │   │   └── LC_MESSAGES/
│       │   │       ├── messages.mo
│       │   │       └── messages.po
│       │   ├── es/
│       │   │   └── LC_MESSAGES/
│       │   │       ├── messages.mo
│       │   │       └── messages.po
│       │   └── messages.pot
│       ├── sinks/
│       │   ├── __init__.py
│       │   └── definitions.py
│       ├── utils/
│           ├── __init__.py
│           └── files.py
│       ├── __init__.py
│       └── cli.py
├── tests/
│   ├── conftest.py
│   ├── test_cli_smoke.py
│   └── test_entropy_seeds.py
├── .env.example
├── .gitignore
├── babel.cfg
├── CHANGELOG.md
├── LICENSE
├── main.py
├── MANIFEST.in
├── pyproject.toml
├── README.md
├── README_ES.md
├── requirements.txt
└── VERSIONING.md
```

# AGENTS.md - CartolaPy

CartolaPy is a Streamlit application for visualization and analysis of data extracted from the official Cartola FC API.

## Project Overview

- **Python**: 3.10+
- **Package Manager**: uv (see `uv.lock`)
- **UI Framework**: Streamlit
- **Key Dependencies**: pandas, numpy, plotly, aiohttp, aiofiles

## Project Structure

```
CartolaPy/
├── src/
│   ├── enums.py                    # Scout, DataPath, UpdateTablesMsg enums
│   ├── utils.py                    # Utility functions (async HTTP, data processing)
│   ├── atletas_updater.py          # Athletes data updater
│   ├── pontuacoes_updater.py       # Scores updater
│   ├── confrontos_or_mandos_updater.py  # Matches/homestead updater
│   ├── pontos_cedidos_updater.py   # Points ceded updater
│   └── pre_season/
│       ├── dicts_creator.py        # JSON dicts creator
│       └── dfs_creator.py          # DataFrames creator
├── plotter.py                      # Plotting functions
├── CartolaPy.py                    # Main Streamlit application
├── data/                           # Data files (CSV, JSON, Parquet)
└── pyproject.toml
```

## Commands

### Installation
```bash
# Using uv
uv sync

# Using pip
pip install -r requirements.txt
```

### Running the Application
```bash
streamlit run CartolaPy.py
```

### Running Single Tests
No test framework is currently configured. If adding tests, use:
```bash
pytest tests/ -v
pytest tests/test_file.py::test_function  # Single test
```

## Code Style Guidelines

### Type Hints
- Use type hints for all function parameters and return values
- Use `| None` syntax (not `Optional[]`) for nullable types
- Use `tuple[int, int]` (not `Tuple[int, int]`) for generics

```python
# Good
async def get_page_json(url: str) -> dict:
def get_pontuacoes_mando(df: pd.DataFrame, ...) -> dict[int, list[str]]:

# Good - union syntax
row_scouts: tuple | None = None
atletas_df: pd.DataFrame | None = None
```

### Naming Conventions
- **Functions/variables**: `snake_case` (e.g., `get_page_json`, `pontuacoes_df`)
- **Classes**: `PascalCase` (e.g., `ConfrontosOrMandosUpdater`, `PontosCedidosUpdater`)
- **Enums**: `PascalCase` for enum name, `SCREAMING_SNAKE` for members (e.g., `Scout.G`, `DataPath.ATLETAS`)
- **Constants**: `SCREAMING_SNAKE_CASE` (e.g., `PRECO_MIN`, `MAX_CACHE_ENTRIES`)
- **Private methods**: Prefix with `_` (e.g., `_update_clube`, `_read_pontos_cedidos`)

### Imports
Organize imports in three groups with blank lines between:
1. Standard library (`asyncio`, `json`, `os`, `warnings`)
2. Third-party packages (`pandas`, `numpy`, `aiohttp`)
3. Local imports (`from src.enums import ...`, `from src.utils import ...`)

```python
import asyncio
import os
import warnings

import pandas as pd
import numpy as np
import streamlit as st

from src.enums import Scout, DataPath
from src.utils import get_page_json
```

### Async/Await Patterns
- Use `asyncio.gather()` for concurrent operations
- Use `asyncio.to_thread()` for blocking operations
- Use `stqdm` for progress bars in async contexts

```python
await asyncio.gather(
    create_dicts(),
    update_atletas()
)

await asyncio.gather(
    *[
        update_pontuacoes_and_scouts_rodada(rodada, pontuacoes_df, scouts_df)
        for rodada in stqdm(range(1, rodada_atual + 1), ...)
    ]
)
```

### Enums
Use enums for related constants. The codebase uses three main enums:
- `Scout`: Scout types with name and value (e.g., `Scout.G = {'name': 'Gol', 'value': 8}`)
- `DataPath`: Data file paths
- `UpdateTablesMsg`: User-facing messages

### Error Handling
- Use conditional checks rather than try/except where appropriate
- Use `np.isnan()` for NaN checks
- Use `isinstance()` for type checking

```python
if not isinstance(scouts, Mapping) and (scouts is None or np.isnan(scouts)):
    return np.nan

if mercado_json['status_mercado'] != 1 or mercado_json['game_over']:
    st.sidebar.info(UpdateTablesMsg.MERCADO_FECHADO.value)
```

### Pandas Patterns
- Method chaining with `.pipe()` for transformations
- Use `.loc[]` and `.iloc[]` for indexing
- Use `df.assign()` for adding columns
- Use `lambda` in column definitions when needed

```python
df.assign(
    **{
        'Média': np.nanmean(np.array(pontuacoes_df), axis=1, keepdims=True),
        'Desvio Padrão': np.nanstd(np.array(pontuacoes_df), axis=1, keepdims=True),
    }
).dropna(subset=['Média']).pipe(U.atletas_clean_and_filter, ...)
```

### Data Files
Data files are stored in:
- `data/csv/` - CSV files for athletes, clubs, positions, scores, pontuacoes_and_scouts
- `data/json/` - JSON files for dictionaries (clubs, positions, status)
- `data/csv/pontos_cedidos/` - CSV files for points ceded by position (1-6)

### Performance Considerations
- Use `@st.cache_resource` for expensive computations
- Use `AsyncLimiter` for rate limiting async HTTP requests
- Use `rate_limiter = AsyncLimiter(10, 1)` for API calls (10 requests per second)

```python
@st.cache_resource(max_entries=MAX_CACHE_ENTRIES)
def plot_atletas_geral(...):
    ...
```

### API Endpoints
The app uses these Cartola API endpoints:
- `https://api.cartola.globo.com/atletas/mercado` - Athletes market data
- `https://api.cartola.globo.com/atletas/pontuados/{rodada}` - Round scores
- `https://api.cartola.globo.com/partidas/{rodada}` - Match data
- `https://api.cartola.globo.com/mercado/status` - Market status

## Adding New Features

1. For new data updaters, follow the pattern in `src/atletas_updater.py` or `src/pontuacoes_updater.py`
2. For new plotting functions, add to `plotter.py` with `@st.cache_resource`
3. For new enums, add to `src/enums.py`
4. For utility functions, add to `src/utils.py`

## Working Directory

All file operations use relative paths from the project root (`/Users/tuliosouza/repos/CartolaPy`).

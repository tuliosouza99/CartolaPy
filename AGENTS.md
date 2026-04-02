# AGENTS.md

## Project Overview

CartolaPy is a Streamlit application for visualization and analysis of data extracted from the official Cartola FC API (Brazilian football/soccer fantasy app). The project uses Python with async/await patterns, pandas, numpy, and plotly for data visualization.

## Project Structure

```
CartolaPy/
├── backend/
│   ├── __init__.py
│   ├── main.py               # FastAPI entry point
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py         # REST API endpoints
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py         # Settings and constants
│   │   ├── scheduler.py      # APScheduler job definitions
│   │   └── lifespan.py       # Startup/shutdown handlers
│   ├── services/
│   │   ├── __init__.py
│   │   └── updater.py        # Unified data update service
│   ├── src/                  # Data logic (moved from original src/)
│   │   ├── enums.py
│   │   ├── utils.py
│   │   ├── atletas_updater.py
│   │   ├── confrontos_or_mandos_updater.py
│   │   ├── pontos_cedidos_updater.py
│   │   ├── pontuacoes_updater.py
│   │   └── pre_season/
│   │       ├── dfs_creator.py
│   │       └── dicts_creator.py
│   └── data/                  # Persisted data files
├── frontend/
│   ├── __init__.py
│   └── streamlit_app.py      # Thin Streamlit client (HTTP calls only)
├── CartolaPy.py               # Original monolithic app (deprecated)
├── plotter.py                 # Plotting functions (shared)
├── data/                      # Runtime data (gitignored)
├── tests/                     # Test files
└── pyproject.toml            # Project configuration
```

## Build/Lint/Test Commands

### Environment Setup
```bash
# Create conda environment (as specified in README)
conda create -n cartolapy python=3.10
conda activate cartolapy
pip install -r requirements.txt

# Or using the existing venv
source .venv/bin/activate
```

### Linting and Formatting
```bash
# Run ruff linter
ruff check .

# Run ruff formatter (check only)
ruff format --check .

# Auto-fix and format
ruff check --fix .
ruff format .
```

### Running the Application
```bash
# Start the backend (FastAPI server on port 8000)
cd backend && uvicorn main:app --reload --port 8000

# In a separate terminal, start the frontend (Streamlit on port 8501)
streamlit run frontend/streamlit_app.py

# Or run both with concurrent processes
cd backend && uvicorn main:app --port 8000 &
streamlit run frontend/streamlit_app.py
```

### Testing
```bash
# Run all tests
pytest

# Run a single test file
pytest tests/test_file.py

# Run tests matching a pattern
pytest -k "test_pattern"

# Run with verbose output
pytest -v

# Run with coverage (if added)
pytest --cov=src --cov-report=html
```

## Code Style Guidelines

### Import Organization
Organize imports in three groups with blank lines between:
1. Standard library imports
2. Third-party imports (aiohttp, pandas, numpy, etc.)
3. Local/application imports

```python
# Standard library
import json
import asyncio
from collections.abc import Mapping
from typing import Iterable

# Third-party
import aiohttp
import aiofiles
import numpy as np
import pandas as pd
from aiolimiter import AsyncLimiter
from stqdm import stqdm

# Local imports
from src.enums import Scout, DataPath
from src.utils import get_page_json
```

### Type Hints
- Use type hints for function parameters and return types
- Use `|` for union types (Python 3.10+): `dict[int, str]`
- Use `None` instead of `Optional[T]` for optional parameters
- Use `tuple` for fixed-length tuples

```python
# Good
async def get_page_json(url: str) -> dict:
def load_dict(name: str) -> dict[int, str]:
def create_df(atletas: pd.DataFrame) -> pd.DataFrame:

# Union types
def get_basic_points(scouts: dict | float | None):
def update_table(self, rodadas: int | Iterable[int]):
```

### Naming Conventions
- **Classes**: `PascalCase` (e.g., `Scout`, `ConfrontosOrMandosUpdater`)
- **Functions/methods**: `snake_case` (e.g., `get_page_json`, `load_dict_async`)
- **Variables**: `snake_case` (e.g., `pontuacoes_df`, `clube_id`)
- **Constants**: `SCREAMING_SNAKE_CASE` or descriptive lowercase (e.g., `PRECO_MIN`, `rate_limiter`)
- **Enums**: `PascalCase` for enum names, `SCREAMING_SNAKE_CASE` for values by convention
- **Avoid single-letter variable names** except in comprehensions or well-known contexts

### Enum Usage
```python
class Scout(Enum):
    G = {'name': 'Gol', 'value': 8}
    A = {'name': 'Assistência', 'value': 5}
    # ... other scouts

    @classmethod
    def as_basic_scouts_list(cls):
        return [
            scout.name
            for scout in cls
            if scout.name not in ('G', 'A', 'FT', 'PP', 'DP', 'SG', 'CV', 'GC')
        ]

class DataPath(Enum):
    ATLETAS = 'data/csv/atletas.csv'
    # ... other paths

    @classmethod
    def as_list(cls):
        return [path.value for path in cls]
```

### Async/Await Patterns
- Use `async/await` for I/O-bound operations (API calls, file operations)
- Use `asyncio.gather()` for concurrent operations
- Use `asyncio.to_thread()` for CPU-bound operations in async context
- Use `aiofiles` for async file operations

```python
async def update_atletas():
    json = await get_page_json('https://api.cartola.globo.com/atletas/mercado')
    # ...

async def update_tables(rodada: list[int] | int):
    await asyncio.gather(
        create_dicts(),
        update_atletas(),
        # ...
    )
```

### Error Handling
- Use specific exception types when catching
- Handle None and NaN values explicitly
- Use `np.isnan()` for numpy floats, `isinstance(x, Mapping)` for dict-like objects

```python
def get_basic_points(scouts: dict | float | None):
    if not isinstance(scouts, Mapping) and (scouts is None or np.isnan(scouts)):
        return np.nan
    # ...
```

### Data Processing with Pandas
- Method chaining with `.pipe()` for readable transformations
- Use `.assign()` for adding columns
- Use `df.loc[]` and `df.iloc[]` for conditional updates
- Use `stqdm` for progress bars in async loops

```python
df = (
    pd.DataFrame(json['atletas'])
    .sort_values(by=['atleta_id'])
    .reset_index(drop=True)
    .pipe(some_function, arg1, arg2)
)

# Async with progress
await asyncio.gather(
    *[update_function(rodada) for rodada in stqdm(range(1, 38), desc='Updating...')]
)
```

### Streamlit Usage
- Use `st.cache_resource` for expensive computations that don't change per-user
- Use containers and expanders for organization
- Use sidebar for controls

```python
@st.cache_resource(max_entries=MAX_CACHE_ENTRIES)
def plot_atletas_geral(atletas_df: pd.DataFrame, ...):
    # cached function
    ...
```

## Current Linting Issues

Running `ruff check .` produces pre-existing errors in original code:
- **F821**: Undefined name `empty_df` in `backend/src/pre_season/dfs_creator.py` (lines 31, 33, 35, 49, 51)
- **F821**: Undefined names `row_pontuacoes` and `row_scouts` in `backend/src/utils.py` (lines 59-75)

These are bugs in the original code that were present before refactoring.

## Development Notes

- The project uses `uv.lock` suggesting `uv` for dependency management
- Data files are stored in `data/` directory (gitignored)
- Uses Brazilian Portuguese in some comments and UI strings
- API calls are rate-limited with `AsyncLimiter(10, 1)` (10 requests per second)

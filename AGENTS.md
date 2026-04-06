# AGENTS.md

CartolaPy is a full-stack app for visualizing/analysis of Cartola FC API data. Backend: FastAPI + Redis + taskiq. Frontend: React + Vite.

## Project Structure
```
backend/
├── main.py              # FastAPI app factory (get_app())
├── lifespan.py          # Startup/shutdown handlers
├── dependencies.py      # FastAPI DI
├── tkq.py               # Taskiq broker
├── tkq_sched.py         # Taskiq scheduler
├── tasks.py             # Background tasks
├── api/
│   ├── routes.py        # REST endpoints
│   └── models.py        # Pydantic models
└── services/
    ├── atletas_unified.py
    ├── enums.py         # Scout enum, data paths
    ├── redis_store.py
    ├── request_handler.py
    └── data_loaders/    # Data loading from Redis

frontend/
├── src/
│   ├── App.jsx          # Main app with routing
│   ├── pages/           # Page components
│   └── components/      # Reusable components
├── package.json
└── vite.config.js

tests/                   # Pytest suite
├── conftest.py          # Shared fixtures
├── test_api_routes.py
└── ... (other test files)
```

## Build/Lint/Test Commands

### Environment
```bash
uv sync && source .venv/bin/activate  # or: source .venv/bin/activate
```

### Python
```bash
ruff check .                    # lint
ruff format --check .           # check formatting
ruff check --fix . && ruff format .  # auto-fix

pytest                          # run all tests
pytest tests/test_api_routes.py # single file
pytest -k "pattern"             # matching pattern
pytest -v                       # verbose
pytest --cov=backend            # with coverage
```

### Frontend
```bash
cd frontend
npm install
npm run dev      # dev server (port 5173)
npm run build    # production build
```

### Running
```bash
docker-compose up -d redis     # start Redis
cd backend && uvicorn main:app --reload --port 8000  # backend
cd frontend && npm run dev    # frontend (separate terminal)
```

## Code Style

### Python Imports (3 groups, blank line between)
```python
from datetime import datetime, timezone
from typing import Annotated, Literal

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query

from backend.services.atletas_unified import compute_atletas_unified
```

### Type Hints
- Use `|` for unions: `dict[int, str]`, `str | None`
- Use `Annotated` for FastAPI DI
- Use `Literal` for exhaustive string unions

### Naming
- **Classes**: `PascalCase` (DataLoader, Scout)
- **Functions/methods**: `snake_case` (get_atletas, compute_atletas_unified)
- **Variables**: `snake_case` (atletas_df, paginated_df)
- **Constants**: `SCREAMING_SNAKE_CASE` or lowercase

### Async/Await
```python
async def update_atletas():
    await data_loader.atletas.fill_atletas()
    data_loader.atletas.save_to_redis(store)

async def fetch_partidas_from_cartola(request_handler, rodada: int) -> list[dict]:
    page_json = await request_handler.make_get_request(
        f"https://api.cartola.globo.com/partidas/{rodada}"
    )
```

### Error Handling
- Raise `HTTPException` for API errors
- Use specific exception types
- Handle None/NaN explicitly: `pd.notna()`, `isinstance()`

```python
if sort_by is not None:
    if sort_by not in df.columns:
        raise HTTPException(status_code=422, detail=f"Invalid sort_by: {sort_by}")
```

### Pandas
```python
df = (
    atletas_df.sort_values("rodada_id", ascending=False)
    .drop_duplicates(subset=["atleta_id"], keep="first")
    .merge(pont_agg, on="atleta_id", how="left")
)
offset = (page - 1) * page_size
paginated_df = df.iloc[offset : offset + page_size]
```

### React/Frontend
- Functional components with hooks
- React Context for global state (theme)
- CSS variables in `index.css` for theming
- Use `data-theme` attribute for dark/light mode

```jsx
import { useState, useEffect } from 'react'

function App() {
  const [theme, setTheme] = useState('dark')
  // ...
}
```

## Testing
- `pytest` with `pytest-asyncio` for async
- `conftest.py` fixtures: `anyio_backend`, `fastapi_app`, `init_taskiq_deps`
- Use `unittest.mock.AsyncMock` and `MagicMock`
- Use `fastapi.testclient.TestClient` for API testing

```python
@pytest.fixture
def fastapi_app():
    from backend.main import get_app
    return get_app()

def test_atletas_returns_correct_structure(client):
    response = client.get("/api/tables/atletas")
    assert response.status_code == 200
```

## Notes
- **Redis**: Required; use `docker-compose up -d redis`
- **Taskiq**: Async task queue for background jobs
- **Rate Limiting**: ~10 req/sec via `AsyncLimiter`
- **Environment**: Tests run with `ENVIRONMENT=pytest`

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
    ├── pontos_cedidos_unified.py
    ├── enums.py         # Scout enum, data paths
    ├── redis_store.py
    ├── request_handler.py
    └── data_loaders/    # Data loading from Redis

frontend/
├── src/
│   ├── App.jsx          # Main app with routing
│   ├── pages/           # Page components
│   │   ├── AtletasUnified.jsx
│   │   ├── PontosCedidosUnified.jsx
│   │   └── ...
│   └── components/      # Reusable components
│       ├── TableView.jsx
│       ├── FilterSidebar.jsx
│       ├── RoundIntervalSlider.jsx
│       ├── Navbar.jsx
│       └── ...
├── package.json
└── vite.config.js

tests/                   # Pytest suite
├── conftest.py          # Shared fixtures
├── test_api_routes.py
└── ... (other test files)
```

## Build/Lint/Test Commands

### Environment Setup
```bash
uv sync && source .venv/bin/activate  # or: source .venv/bin/activate
```

### Python
```bash
ruff check .                    # lint entire project
ruff format --check .           # check formatting
ruff check --fix . && ruff format .  # auto-fix lint and format

pytest                          # run all tests
pytest tests/test_api_routes.py # single file
pytest -k "pattern"             # matching pattern
pytest -v                       # verbose output
pytest --cov=backend            # with coverage
```

### Frontend
```bash
cd frontend
npm install
npm run dev      # dev server (port 5173)
npm run build    # production build
npm run preview  # preview production build
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
- Avoid `Optional[X]` in favor of `X | None`

### Naming Conventions
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

## Frontend State Persistence (Filter/Sort State)

### Overview
Filter and sort state persist across page navigation using a dual-sessionStorage + URL params strategy. This ensures state survives page reloads and bookmarking.

### Persistence Strategy
1. **URL params** - Primary for shareable/bookmarkable state
2. **sessionStorage** - Fallback for in-session navigation
3. **Default values** - Only non-default values appear in URL

### Filter State Keys
| State Key | Default | URL Param | Notes |
|-----------|---------|-----------|-------|
| `rodadaRange.min` | `1` | `rodada_min` | |
| `rodadaRange.max` | `statusData.rodada_atual` | `rodada_max` | |
| `isMandante` | `"geral"` | `is_mandante` | Values: `"geral"`, `"mandante"`, `"visitante"` |
| `filters.search` | `""` | `search` | |
| `filters.clube_ids` | `[]` | `clube_ids` | CSV: `"1,2,3"` |
| `filters.posicao_ids` | `[]` | `posicao_ids` | CSV |
| `filters.status_ids` | `[]` | `status_ids` | CSV |
| `filters.preco_min` | `0` | `preco_min` | |
| `filters.preco_max` | `30` | `preco_max` | |
| `sortBy` | `"media"` | `sort_by` | Atletas |
| `sortDirection` | `"desc"` | `sort_direction` | Values: `"asc"`, `"desc"` |

### URL Restore Pattern
Use `urlRestored` ref to prevent premature URL updates before initial restore completes:

```javascript
const urlRestored = useRef(false);

useEffect(() => {
  // Fetch initial data and restore URL state
  fetchStatus();
  fetchFilterOptions();
}, []);

useEffect(() => {
  // Parse URL params on mount
  const params = new URLSearchParams(window.location.search);
  const urlState = {};
  if (params.get("rodada_min")) urlState.rodada_min = parseInt(params.get("rodada_min"), 10);
  // ... parse other params

  // Also check sessionStorage as fallback
  if (Object.keys(urlState).length === 0) {
    const saved = sessionStorage.getItem(ROUTE);
    if (saved) {
      // parse saved params
    }
  }

  // Apply restored state
  if (Object.keys(urlState).length > 0) {
    setRodadaRange({ min: urlState.rodada_min ?? 1, max: urlState.rodada_max ?? 1 });
    setFilters(prev => ({ ...prev, ...urlState }));
    if (urlState.sort_by) setSortBy(urlState.sort_by);
    if (urlState.sort_direction) setSortDirection(urlState.sort_direction);
  }
  urlRestored.current = true;
}, []);
```

### URL Sync Effect
Only write to URL after initial restore is complete:

```javascript
useEffect(() => {
  if (!urlRestored.current) return;

  const params = new URLSearchParams();
  if (rodadaRange.min !== 1) params.set("rodada_min", rodadaRange.min);
  if (rodadaRange.max !== statusData?.rodada_atual) params.set("rodada_max", rodadaRange.max);
  if (isMandante !== "geral") params.set("is_mandante", isMandante);
  if (filters.search) params.set("search", filters.search);
  if (filters.clube_ids?.length) params.set("clube_ids", filters.clube_ids.join(","));
  // ... other non-default params

  const queryString = params.toString();
  sessionStorage.setItem(ROUTE, queryString);

  const newUrl = queryString ? `${window.location.pathname}?${queryString}` : window.location.pathname;
  window.history.replaceState({}, "", newUrl);
}, [rodadaRange, isMandante, filters, statusData, sortBy, sortDirection]);
```

### "Redefinir Filtros" (Reset Filters) Pattern
```javascript
const redefinirFiltros = useCallback(() => {
  setRodadaRange({ min: 1, max: statusData?.rodada_atual || 1 });
  setIsMandante("geral");
  setFilters(prev => ({
    ...DEFAULT_FILTERS,
    options: prev.options,  // preserve loaded options
  }));
  setSortBy("media");
  setSortDirection("desc");
  sessionStorage.removeItem(ROUTE);
  window.history.replaceState({}, "", window.location.pathname);
}, [statusData]);
```

### Navigation Pattern
**IMPORTANT**: Use `window.location.href` instead of React Router `<Link>` for page navigation. React Router with `<Link>` does not cause component remounting, which means useEffect hooks don't re-fire and state doesn't re-restore from URL.

```javascript
// In Navbar.jsx
<button onClick={() => window.location.href = '/atletas'}>
  Atletas
</button>

// NOT: <Link to="/atletas">Atletas</Link>
```

### Sort State in TableView
TableView should receive controlled sort props from parent. Internal table sort state resets on re-render, so lift state to parent:

```javascript
// Parent (page component)
const [sortBy, setSortBy] = useState("media");
const [sortDirection, setSortDirection] = useState("desc");

// Pass to TableView
<TableView
  data={data}
  sortBy={sortBy}
  sortDirection={sortDirection}
  onSortChange={(col) => {
    if (col === sortBy) {
      setSortDirection(prev => prev === "asc" ? "desc" : "asc");
    } else {
      setSortBy(col);
      setSortDirection("desc");
    }
  }}
/>

// TableView receives and uses props
function TableView({ data, sortBy, sortDirection, onSortChange }) {
  // Use sortBy/sortDirection from props, not internal state
  const sortedData = useMemo(() => {
    return [...data].sort((a, b) => {
      const aVal = a[sortBy];
      const bVal = b[sortBy];
      // ...
    });
  }, [data, sortBy, sortDirection]);
}
```

### Default Sort Values
- **AtletasUnified**: `sortBy="media"`, `sortDirection="desc"`
- **PontosCedidosUnified**: `sortBy="media_cedida"`, `sortDirection="desc"`

## React/Frontend Conventions
- Functional components with hooks
- React Context for global state (theme)
- CSS variables in `index.css` for theming
- Use `data-theme` attribute for dark/light mode
- Use `useCallback` for event handlers passed to children
- Use `useMemo` for expensive computations

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
- **Backend hot reload**: `uvicorn main:app --reload` works for most changes
- **Docker rebuild**: Use `docker compose up -d --build` to force container rebuild

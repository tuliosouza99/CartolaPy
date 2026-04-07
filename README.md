# CartolaPy

A full-stack application for visualizing and analyzing data from the Cartola FC fantasy football API.

## Tech Stack

**Backend**: FastAPI + Redis + Taskiq  
**Frontend**: React + Vite

## Prerequisites

- Python 3.10+
- Node.js 18+
- Docker (for Redis)

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/tuliosouza99/CartolaPy.git
cd CartolaPy

# 2. Start Redis
docker-compose up -d redis

# 3. Backend setup
uv sync && source .venv/bin/activate
cd backend && uvicorn main:app --reload --port 8000

# 4. Frontend setup (in a new terminal)
cd frontend
npm install
npm run dev
```

Open http://localhost:5173 in your browser.

## Project Structure

```
backend/
├── main.py              # FastAPI app factory
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
    ├── enums.py
    ├── redis_store.py
    ├── request_handler.py
    └── data_loaders/

frontend/
├── src/
│   ├── App.jsx
│   ├── pages/
│   │   ├── AtletasUnified.jsx
│   │   ├── PontosCedidosUnified.jsx
│   │   └── ...
│   └── components/
│       ├── TableView.jsx
│       ├── FilterSidebar.jsx
│       ├── Navbar.jsx
│       └── ...

tests/                   # Pytest suite
```

## Available Scripts

### Backend
```bash
ruff check .                    # Lint
ruff format --check .           # Check formatting
ruff check --fix . && ruff format .  # Auto-fix

pytest                          # Run tests
pytest --cov=backend            # With coverage
```

### Frontend
```bash
npm run dev      # Dev server (port 5173)
npm run build    # Production build
npm run preview  # Preview production build
```

## Features

- **AtletasUnified**: View player statistics with filters for position, club, status, and price range
- **PontosCedidosUnified**: View points ceded by each team for specific positions
- **Confrontos**: Match predictions and analysis
- **Pontuacoes**: Round-by-round scoring data
- Filter by round range, home/away conditions
- Sort and paginate results
- Dark/light theme toggle
- URL-based filter state (shareable/bookmarkable links)

## Environment Variables

- `ENVIRONMENT=pytest` for test mode
- Redis connection required for data storage

## API Endpoints

- `GET /api/tables/atletas` - Player data
- `GET /api/tables/pontos-cedidos` - Points ceded data
- `GET /api/status` - Current round status
- `POST /api/update/atletas` - Trigger data update
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
CSV_DIR = DATA_DIR / "csv"
JSON_DIR = DATA_DIR / "json"
PARQUET_DIR = DATA_DIR / "parquet"
PONTOS_CEDIDOS_DIR = DATA_DIR / "csv" / "pontos_cedidos"

MARKET_STATUS_URL = "https://api.cartola.globo.com/mercado/status"
ATLETAS_URL = "https://api.cartola.globo.com/atletas/mercado"
PONTUADOS_URL = "https://api.cartola.globo.com/atletas/pontuados/{rodada}"
PARTIDAS_URL = "https://api.cartola.globo.com/partidas/{rodada}"

SCHEDULER_INTERVAL_MINUTES = 5

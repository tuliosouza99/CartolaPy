import { useState, useEffect, useCallback, useRef } from "react";

const SCOUTS_DISPLAY = [
  "G", "A", "FT", "FD", "FF", "FS", "PS", "V", "I", "PP",
  "DS", "SG", "DE", "DP", "CV", "CA", "FC", "GC", "GS", "PC",
];

function Confrontos() {
  const [statusData, setStatusData] = useState(null);
  const [rodada, setRodada] = useState(1);
  const [matches, setMatches] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedMatches, setExpandedMatches] = useState(new Set());
  const [showSelect, setShowSelect] = useState(false);
  const urlRestored = useRef(false);
  const initialLoadComplete = useRef(false);

  const ROUTE = "/confrontos";

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    let urlState = {};
    if (params.get("rodada")) {
      urlState.rodada = parseInt(params.get("rodada"), 10);
    }

    if (Object.keys(urlState).length === 0) {
      const saved = sessionStorage.getItem(ROUTE);
      if (saved) {
        const savedParams = new URLSearchParams(saved);
        if (savedParams.get("rodada")) {
          urlState.rodada = parseInt(savedParams.get("rodada"), 10);
        }
      }
    }

    if (urlState.rodada) {
      setRodada(urlState.rodada);
    }
    urlRestored.current = true;
  }, []);

  useEffect(() => {
    fetchStatus();
  }, []);

  useEffect(() => {
    if (!urlRestored.current) return;
    if (!statusData) return;
    if (!initialLoadComplete.current) return;

    fetchConfrontos(rodada);
  }, [rodada, statusData]);

  useEffect(() => {
    if (!urlRestored.current) return;
    if (!statusData) return;
    if (!initialLoadComplete.current) return;

    const params = new URLSearchParams();
    if (rodada !== statusData.rodada_atual) params.set("rodada", rodada);

    const queryString = params.toString();
    sessionStorage.setItem(ROUTE, queryString);

    const newUrl = queryString
      ? `${window.location.pathname}?${queryString}`
      : window.location.pathname;
    window.history.replaceState({}, "", newUrl);
  }, [rodada, statusData]);

  const fetchStatus = async () => {
    try {
      const res = await fetch("/api/tables/status");
      if (res.ok) {
        const data = await res.json();
        setStatusData(data);
        const params = new URLSearchParams(window.location.search);
        const hasUrlRodada = params.get("rodada");
        const saved = sessionStorage.getItem(ROUTE);
        const hasSavedRodada = saved && new URLSearchParams(saved).get("rodada");
        const targetRodada = hasUrlRodada || hasSavedRodada
          ? rodada
          : (data.rodada_atual || 1);
        if (!hasUrlRodada && !hasSavedRodada) {
          setRodada(targetRodada);
        }
        fetchConfrontos(targetRodada);
        initialLoadComplete.current = true;
      }
    } catch (err) {
      console.error("Failed to fetch status:", err);
    }
  };

  const fetchConfrontos = async (rodadaNum) => {
    setLoading(true);
    try {
      const res = await fetch(`/api/confrontos/${rodadaNum}`);
      if (res.ok) {
        const data = await res.json();
        setMatches(data.matches || []);
      }
    } catch (err) {
      console.error("Failed to fetch confrontos:", err);
      setMatches([]);
    } finally {
      setLoading(false);
    }
  };

  const goToPreviousRodada = useCallback(() => {
    setRodada((prev) => Math.max(1, prev - 1));
  }, []);

  const goToNextRodada = useCallback(() => {
    if (!statusData) return;
    setRodada((prev) => Math.min(statusData.rodada_atual, prev + 1));
  }, [statusData]);

  const toggleMatch = useCallback((idx) => {
    setExpandedMatches((prev) => {
      const next = new Set(prev);
      if (next.has(idx)) {
        next.delete(idx);
      } else {
        next.add(idx);
      }
      return next;
    });
  }, []);

  const formatDate = (dateStr) => {
    if (!dateStr) return null;
    const date = new Date(dateStr);
    return date.toLocaleString("pt-BR", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "America/Sao_Paulo",
      timeZoneName: "short",
    });
  };

  const renderScouts = (scouts) => {
    const entries = Object.entries(scouts).filter(([_, v]) => v !== 0);
    if (entries.length === 0) return null;
    return entries.map(([k, v]) => `${v} ${k}`).join(" ");
  };

  const renderPlayers = (players, side) => (
    <div
      style={{
        flex: 1,
        minWidth: 0,
        display: "flex",
        flexDirection: "column",
        gap: "0.25rem",
      }}
    >
      {players.map((player) => (
        <div
          key={player.atleta_id}
          style={{
            display: "flex",
            flexDirection: "column",
            gap: "0.125rem",
            fontSize: "0.8rem",
            padding: "0.25rem 0.5rem",
            background: "rgba(255,255,255,0.03)",
            borderRadius: "var(--radius-sm)",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "stretch",
              gap: "0.5rem",
            }}
          >
            <span
              style={{
                width: "1.5rem",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontWeight: 600,
                color: "var(--text-secondary)",
                fontSize: "0.7rem",
              }}
            >
              {player.posicao_abreviacao}
            </span>
            <span
              style={{
                flex: 1,
                display: "flex",
                alignItems: "center",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                color: "var(--text-primary)",
              }}
            >
              {player.apelido}
            </span>
            <span
              style={{
                display: "flex",
                alignItems: "center",
                fontWeight: 600,
                color: "var(--orange)",
                textAlign: "right",
              }}
            >
              {player.pontuacao.toFixed(1)}
            </span>
          </div>
          <div
            style={{
              display: "flex",
              alignItems: "stretch",
              gap: "0.5rem",
              paddingLeft: "2rem",
            }}
          >
            <span
              style={{
                fontSize: "0.65rem",
                color: "var(--text-muted)",
              }}
            >
              Básica: {player.pontuacao_basica.toFixed(1)}
            </span>
            {player.scouts && Object.keys(player.scouts).length > 0 && (
              <span
                style={{
                  flex: 1,
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.65rem",
                  color: "var(--text-secondary)",
                  textAlign: "right",
                }}
              >
                {renderScouts(player.scouts)}
              </span>
            )}
          </div>
        </div>
      ))}
    </div>
  );

  const maxRodada = statusData?.rodada_atual || 1;

  return (
    <div>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: "1.5rem",
        }}
      >
        <h1
          style={{
            fontFamily: "var(--font-display)",
            fontSize: "1.75rem",
            fontWeight: 700,
            color: "var(--text-primary)",
            letterSpacing: "-0.02em",
          }}
        >
          Confrontos
        </h1>
        <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
          <button
            onClick={goToPreviousRodada}
            disabled={rodada <= 1}
            style={{
              width: "2rem",
              height: "2rem",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              background: "var(--bg-card)",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius-sm)",
              color: rodada <= 1 ? "var(--text-muted)" : "var(--text-primary)",
              cursor: rodada <= 1 ? "not-allowed" : "pointer",
              fontSize: "0.875rem",
              transition: "all var(--transition)",
            }}
          >
            ‹
          </button>

          <div style={{ position: "relative" }}>
            <button
              onClick={() => setShowSelect(!showSelect)}
              style={{
                padding: "0.5rem 1rem",
                background: "var(--bg-card)",
                border: "1px solid var(--border)",
                borderRadius: "var(--radius-sm)",
                color: "var(--text-primary)",
                cursor: "pointer",
                fontFamily: "var(--font-display)",
                fontWeight: 600,
                fontSize: "0.875rem",
                minWidth: "8rem",
                textAlign: "center",
              }}
            >
              {rodada}ª rodada
            </button>
            {showSelect && (
              <select
                value={rodada}
                onChange={(e) => {
                  setRodada(parseInt(e.target.value, 10));
                  setShowSelect(false);
                }}
                onBlur={() => setShowSelect(false)}
                autoFocus
                style={{
                  position: "absolute",
                  top: 0,
                  left: 0,
                  width: "100%",
                  height: "100%",
                  opacity: 0,
                  cursor: "pointer",
                }}
              >
                {Array.from({ length: maxRodada }, (_, i) => i + 1).map((r) => (
                  <option key={r} value={r}>
                    {r}ª rodada
                  </option>
                ))}
              </select>
            )}
          </div>

          <button
            onClick={goToNextRodada}
            disabled={rodada >= maxRodada}
            style={{
              width: "2rem",
              height: "2rem",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              background: "var(--bg-card)",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius-sm)",
              color:
                rodada >= maxRodada ? "var(--text-muted)" : "var(--text-primary)",
              cursor: rodada >= maxRodada ? "not-allowed" : "pointer",
              fontSize: "0.875rem",
              transition: "all var(--transition)",
            }}
          >
            ›
          </button>
        </div>
      </div>

      {loading ? (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: "4rem",
            color: "var(--text-muted)",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
            <div
              style={{
                width: "20px",
                height: "20px",
                border: "2px solid var(--border)",
                borderTopColor: "var(--orange)",
                borderRadius: "50%",
                animation: "spin 0.8s linear infinite",
              }}
            />
            Carregando...
          </div>
        </div>
      ) : matches.length === 0 ? (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: "4rem",
            color: "var(--text-muted)",
          }}
        >
          Nenhum dado encontrado
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
          {matches.map((match, idx) => (
            <div
              key={`${match.mandante_id}-${match.visitante_id}-${idx}`}
              style={{
                background: "var(--bg-card)",
                borderRadius: "var(--radius-lg)",
                border: "1px solid var(--border)",
                overflow: "hidden",
                boxShadow: "var(--shadow-md)",
              }}
            >
              <div
                style={{
                  padding: "1rem 1.25rem",
                  cursor: "pointer",
                  display: "flex",
                  alignItems: "center",
                  gap: "1rem",
                }}
                onClick={() => toggleMatch(idx)}
              >
                <span
                  style={{
                    fontSize: "0.75rem",
                    color: "var(--text-muted)",
                    marginRight: "0.5rem",
                  }}
                >
                  {expandedMatches.has(idx) ? "▼" : "▶"}
                </span>
                <div
                  style={{
                    flex: 1,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "flex-end",
                    gap: "0.75rem",
                  }}
                >
                  <span
                    style={{
                      fontFamily: "var(--font-display)",
                      fontWeight: 600,
                      fontSize: "0.875rem",
                      color: "var(--text-primary)",
                      textAlign: "right",
                      flex: 1,
                    }}
                  >
                    {match.mandante_nome}
                  </span>
                  <img
                    src={match.mandante_escudo}
                    alt="mandante"
                    style={{
                      width: "28px",
                      height: "28px",
                      objectFit: "contain",
                    }}
                    onError={(e) => {
                      e.target.style.display = "none";
                    }}
                  />
                  <span
                    style={{
                      fontFamily: "var(--font-display)",
                      fontWeight: 700,
                      fontSize: "1.1rem",
                      color: "var(--text-primary)",
                      minWidth: "1.5rem",
                      textAlign: "center",
                    }}
                  >
                    {match.placar_mandante ?? "-"}
                  </span>
                  <span
                    style={{
                      color: "var(--text-muted)",
                      fontWeight: 300,
                      fontSize: "1rem",
                    }}
                  >
                    x
                  </span>
                  <span
                    style={{
                      fontFamily: "var(--font-display)",
                      fontWeight: 700,
                      fontSize: "1.1rem",
                      color: "var(--text-primary)",
                      minWidth: "1.5rem",
                      textAlign: "center",
                    }}
                  >
                    {match.placar_visitante ?? "-"}
                  </span>
                  <img
                    src={match.visitante_escudo}
                    alt="visitante"
                    style={{
                      width: "28px",
                      height: "28px",
                      objectFit: "contain",
                    }}
                    onError={(e) => {
                      e.target.style.display = "none";
                    }}
                  />
                  <span
                    style={{
                      fontFamily: "var(--font-display)",
                      fontWeight: 600,
                      fontSize: "0.875rem",
                      color: "var(--text-primary)",
                      textAlign: "left",
                      flex: 1,
                    }}
                  >
                    {match.visitante_nome}
                  </span>
                </div>
              </div>

              <div
                style={{
                  padding: "0 1.25rem 0.75rem",
                  display: "flex",
                  justifyContent: "center",
                  gap: "1rem",
                  fontSize: "0.75rem",
                  color: "var(--text-muted)",
                }}
              >
                {match.local && <span>{match.local}</span>}
                {match.local && match.partida_data && <span>•</span>}
                {match.partida_data && <span>{formatDate(match.partida_data)}</span>}
              </div>

              {expandedMatches.has(idx) && (
                <div
                  style={{
                    borderTop: "1px solid var(--border)",
                    padding: "1rem 1.25rem",
                    background: "var(--bg-secondary)",
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      gap: "2rem",
                    }}
                  >
                    {renderPlayers(match.mandante_players, "mandante")}
                    <div
                      style={{
                        width: "1px",
                        background: "var(--border)",
                        alignSelf: "stretch",
                      }}
                    />
                    {renderPlayers(match.visitante_players, "visitante")}
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      <style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
}

export default Confrontos;

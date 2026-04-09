import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import TableView from "../components/TableView";
import RoundIntervalSlider from "../components/RoundIntervalSlider";
import MandoToggle from "../components/MandoToggle";
import ScoutSelect, { SCOUT_BY_CODE } from "../components/ScoutSelect";
import PontosCedidosScoutChart from "../components/PontosCedidosScoutChart";

const SCOUTS_BY_POSITION = {
  1: ["SG", "DP", "DE", "GS", "CA", "CV", "FC", "GC", "PC"],
  2: ["G", "A", "FT", "FD", "FF", "FS", "PS", "V", "I", "PP", "DS", "SG", "DE", "CA", "CV", "FC", "GC", "GS", "PC"],
  3: ["G", "A", "FT", "FD", "FF", "FS", "PS", "V", "I", "PP", "DS", "SG", "DE", "CA", "CV", "FC", "GC", "GS", "PC"],
  4: ["G", "A", "FT", "FD", "FF", "FS", "PS", "V", "I", "PP", "DS", "SG", "DE", "CA", "CV", "FC", "GC", "GS", "PC"],
  5: ["G", "A", "FT", "FD", "FF", "FS", "PS", "V", "I", "PP", "DS", "SG", "DE", "CA", "CV", "FC", "GC", "GS", "PC"],
  6: ["V"],
};

function PontosConquistadosUnified() {
  const [statusData, setStatusData] = useState(null);
  const [rodadaRange, setRodadaRange] = useState({ min: 1, max: 1 });
  const [isMandante, setIsMandante] = useState("geral");
  const [selectedPosition, setSelectedPosition] = useState(1);
  const [filterOptions, setFilterOptions] = useState({ clubes: {}, posicoes: [], status: [] });
  const [selectedStatusIds, setSelectedStatusIds] = useState([]);
  const [sortBy, setSortBy] = useState("media_conquistada");
  const [sortDirection, setSortDirection] = useState("desc");
  const [scout, setScout] = useState(null);
  const [expandedMatches, setExpandedMatches] = useState({});
  const [expandedRows, setExpandedRows] = useState(new Set());
  const loadingMatchesRef = useRef(new Set());

  const urlRestored = useRef(false);

  const ROUTE = "/pontos-conquistados";

  useEffect(() => {
    fetchStatus();
    fetchFilterOptions();
  }, []);

  useEffect(() => {
    let params = new URLSearchParams(window.location.search);
    const urlState = {};
    if (params.get("rodada_min")) urlState.rodada_min = parseInt(params.get("rodada_min"), 10);
    if (params.get("rodada_max")) urlState.rodada_max = parseInt(params.get("rodada_max"), 10);
    if (params.get("is_mandante")) urlState.is_mandante = params.get("is_mandante");
    if (params.get("posicao_id")) urlState.posicao_id = parseInt(params.get("posicao_id"), 10);
    if (params.get("sort_by")) urlState.sort_by = params.get("sort_by");
    if (params.get("sort_direction")) urlState.sort_direction = params.get("sort_direction");
    if (params.get("status_ids")) urlState.status_ids = params.get("status_ids").split(",").map(Number).filter(Boolean);
    if (params.get("scout")) urlState.scout = params.get("scout");

    if (Object.keys(urlState).length === 0) {
      const saved = sessionStorage.getItem(ROUTE);
      if (saved) {
        params = new URLSearchParams(saved);
        if (params.get("rodada_min")) urlState.rodada_min = parseInt(params.get("rodada_min"), 10);
        if (params.get("rodada_max")) urlState.rodada_max = parseInt(params.get("rodada_max"), 10);
        if (params.get("is_mandante")) urlState.is_mandante = params.get("is_mandante");
        if (params.get("posicao_id")) urlState.posicao_id = parseInt(params.get("posicao_id"), 10);
        if (params.get("sort_by")) urlState.sort_by = params.get("sort_by");
        if (params.get("sort_direction")) urlState.sort_direction = params.get("sort_direction");
        if (params.get("status_ids")) urlState.status_ids = params.get("status_ids").split(",").map(Number).filter(Boolean);
        if (params.get("scout")) urlState.scout = params.get("scout");
      }
    }

    if (urlState.rodada_min) {
      setRodadaRange((prev) => ({ ...prev, min: urlState.rodada_min }));
    }
    if (urlState.rodada_max) {
      setRodadaRange((prev) => ({ ...prev, max: urlState.rodada_max }));
    }
    if (urlState.is_mandante) {
      setIsMandante(urlState.is_mandante);
    }
    if (urlState.posicao_id) {
      setSelectedPosition(urlState.posicao_id);
    }
    if (urlState.sort_by) {
      setSortBy(urlState.sort_by);
    }
    if (urlState.sort_direction) {
      setSortDirection(urlState.sort_direction);
    }
    if (urlState.status_ids) {
      setSelectedStatusIds(urlState.status_ids);
    }
    if (urlState.scout) {
      setScout(urlState.scout);
    }
    urlRestored.current = true;
  }, []);

  useEffect(() => {
    if (!urlRestored.current) return;

    const params = new URLSearchParams();
    if (rodadaRange.min !== 1) params.set("rodada_min", rodadaRange.min);
    if (rodadaRange.max !== (statusData?.rodada_atual ?? 1)) params.set("rodada_max", rodadaRange.max);
    if (isMandante !== "geral") params.set("is_mandante", isMandante);
    if (selectedPosition !== 1) params.set("posicao_id", selectedPosition);
    if (sortBy !== "media_conquistada") params.set("sort_by", sortBy);
    if (sortDirection !== "desc") params.set("sort_direction", sortDirection);
    if (selectedStatusIds.length > 0) params.set("status_ids", selectedStatusIds.join(","));
    if (scout) params.set("scout", scout);

    const queryString = params.toString();
    sessionStorage.setItem(ROUTE, queryString);

    const newUrl = queryString ? `${window.location.pathname}?${queryString}` : window.location.pathname;
    window.history.replaceState({}, "", newUrl);
  }, [rodadaRange, isMandante, selectedPosition, statusData, sortBy, sortDirection, selectedStatusIds, scout]);

  useEffect(() => {
    setScout(null);
    setSortBy("media_conquistada");
    setSortDirection("desc");
  }, [selectedPosition]);

  const redefinirFiltros = useCallback(() => {
    setRodadaRange({ min: 1, max: statusData?.rodada_atual || 1 });
    setIsMandante("geral");
    setSelectedPosition(1);
    setSelectedStatusIds([]);
    setSortBy("media_conquistada");
    setSortDirection("desc");
    setScout(null);
    sessionStorage.removeItem(ROUTE);
    window.history.replaceState({}, "", window.location.pathname);
  }, [statusData]);

  const fetchStatus = async () => {
    try {
      const res = await fetch("/api/tables/status");
      if (res.ok) {
        const data = await res.json();
        setStatusData(data);
        setRodadaRange((prev) => ({ min: prev.min, max: data.rodada_atual || 1 }));
      }
    } catch (err) {
      console.error("Failed to fetch status:", err);
    }
  };

  const fetchFilterOptions = async () => {
    try {
      const res = await fetch("/api/tables/filter-options");
      if (res.ok) {
        const data = await res.json();
        const clubesMap = {};
        (data.clubes || []).forEach((clube) => {
          clubesMap[clube.id] = clube;
        });
        setFilterOptions({
          clubes: clubesMap,
          posicoes: data.posicoes || [],
          status: data.status || [],
        });
      }
    } catch (err) {
      console.error("Failed to fetch filter options:", err);
    }
  };

  const getClubeEscudo = useCallback(
    (clubeId) => {
      const clube = filterOptions.clubes[clubeId];
      if (!clube) return "";
      return clube.escudos?.["60x60"] || "";
    },
    [filterOptions.clubes]
  );

  const getClubeName = useCallback(
    (clubeId) => {
      const clube = filterOptions.clubes[clubeId];
      return clube ? clube.nome_fantasia || `Clube ${clubeId}` : `Clube ${clubeId}`;
    },
    [filterOptions.clubes]
  );

  const columns = useMemo(
    () => {
      const baseColumns = [
        {
          key: "clube_escudo",
          label: "Clube",
          sortable: false,
          renderCell: (row) => (
            <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
              <img
                src={getClubeEscudo(row.clube_id)}
                alt="clube"
                style={{
                  width: "24px",
                  height: "24px",
                  objectFit: "contain",
                }}
                onError={(e) => {
                  e.target.style.display = "none";
                }}
              />
              <span>{getClubeName(row.clube_id)}</span>
            </div>
          ),
        },
        {
          key: "media_conquistada",
          label: "Média Conquistada",
          sortable: true,
          renderCell: (row) => {
            const val = row.media_conquistada;
            if (val == null) return "-";
            return Number(val).toFixed(2);
          },
        },
        {
          key: "media_conquistada_basica",
          label: "Média Conquistada Básica",
          sortable: true,
          renderCell: (row) => {
            const val = row.media_conquistada_basica;
            return val != null ? Number(val).toFixed(2) : "-";
          },
        },
        { key: "total_jogos", label: "Jogos", sortable: true },
      ];

      if (scout && SCOUT_BY_CODE[scout]) {
        const scoutInfo = SCOUT_BY_CODE[scout];
        baseColumns.push({
          key: `scout_${scout}`,
          label: scoutInfo.code,
          sortable: true,
          renderCell: (row) => {
            const scouts = row.scouts || {};
            const value = scouts[scout] || 0;
            return (
              <span style={{ fontFamily: "var(--font-mono)", color: "var(--orange)" }}>
                {value}
              </span>
            );
          },
        });
      }

      return baseColumns;
    },
    [scout, getClubeEscudo, getClubeName]
  );

  const fetchMatchData = useCallback(
    async (clubeId, posId) => {
      const key = `${clubeId}-${posId}`;
      if (expandedMatches[key] || loadingMatchesRef.current.has(key)) return;
      loadingMatchesRef.current.add(key);

      try {
        const params = new URLSearchParams({
          rodada_min: rodadaRange.min,
          rodada_max: rodadaRange.max,
          posicao_id: posId,
        });
        if (selectedStatusIds.length > 0) {
          params.set("status_ids", selectedStatusIds.join(","));
        }
        const res = await fetch(
          `/api/tables/pontos-conquistados-unified/${clubeId}/matches?${params}`
        );
        if (res.ok) {
          const data = await res.json();
          setExpandedMatches((prev) => ({ ...prev, [key]: data.matches || [] }));
        }
      } catch (err) {
        console.error("Failed to fetch match data:", err);
      } finally {
        loadingMatchesRef.current.delete(key);
      }
    },
    [rodadaRange, expandedMatches, selectedStatusIds]
  );

  const handleExpandedRowsChange = useCallback(
    (newExpandedRows) => {
      setExpandedRows(newExpandedRows);
    },
    []
  );

  useEffect(() => {
    setExpandedMatches({});
    loadingMatchesRef.current.clear();
  }, [rodadaRange, selectedStatusIds]);

  const renderExpandedContent = useCallback(
    (row) => {
      const scouts = row.scouts || {};
      const positionScouts = SCOUTS_BY_POSITION[selectedPosition] || [];
      const scoutEntries = Object.entries(scouts).filter(
        ([key, value]) =>
          value !== 0 &&
          value != null &&
          positionScouts.includes(key)
      );

      const matchKey = `${row.clube_id}-${selectedPosition}`;
      const allMatchData = expandedMatches[matchKey] || null;

      if (!allMatchData) {
        fetchMatchData(row.clube_id, selectedPosition);
      }

      const matchData = allMatchData
        ? allMatchData.filter((m) => {
            if (isMandante === "mandante" && m.is_mandante !== true) return false;
            if (isMandante === "visitante" && m.is_mandante !== false) return false;
            return true;
          })
        : null;

      if (scoutEntries.length === 0 && !matchData) {
        return (
          <div style={{ padding: "1rem", color: "var(--text-muted)" }}>
            Nenhum scout registrado
          </div>
        );
      }

      return (
        <div style={{ padding: "1rem" }}>
          {row.scout_contributions && Object.keys(row.scout_contributions).length > 0 && (
            <PontosCedidosScoutChart
              scoutContributions={row.scout_contributions}
              mediaCedida={row.media_conquistada}
              positionScouts={positionScouts}
              totalJogos={row.total_jogos}
            />
          )}

          <div
            style={{
              fontFamily: "var(--font-display)",
              fontSize: "0.75rem",
              fontWeight: 600,
              color: "var(--text-secondary)",
              marginBottom: "0.5rem",
              textTransform: "uppercase",
              letterSpacing: "0.05em",
            }}
          >
            Média Conquistada por Confronto
          </div>
          {!matchData ? (
            <div style={{ color: "var(--text-muted)", fontSize: "0.875rem" }}>
              Carregando...
            </div>
          ) : matchData.length === 0 ? (
            <div style={{ color: "var(--text-muted)", fontSize: "0.875rem" }}>
              Nenhum confronto encontrado
            </div>
          ) : (
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(4, 1fr)",
                gap: "0.5rem",
              }}
            >
              {matchData.map((match) => (
                <a
                  key={match.partida_id}
                  href={`/confrontos?rodada=${match.rodada_id}&partida_id=${match.partida_id}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "0.5rem",
                    padding: "0.5rem",
                    background: "var(--bg-card)",
                    borderRadius: "var(--radius-sm)",
                    textDecoration: "none",
                    color: "var(--text-primary)",
                    fontSize: "0.75rem",
                  }}
                >
                  <img
                    src={match.opponent_escudo}
                    alt="opponent"
                    style={{
                      width: "18px",
                      height: "18px",
                      objectFit: "contain",
                    }}
                    onError={(e) => {
                      e.target.style.display = "none";
                    }}
                  />
                  <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {match.opponent_nome}
                  </span>
                  <span
                    style={{
                      padding: "0.125rem 0.25rem",
                      background: match.is_mandante ? "var(--orange)" : "var(--bg-secondary)",
                      color: match.is_mandante ? "white" : "var(--text-secondary)",
                      borderRadius: "var(--radius-sm)",
                      fontSize: "0.625rem",
                      fontWeight: 600,
                    }}
                  >
                    {match.is_mandante ? "M" : "V"}
                  </span>
                  <span style={{ color: "var(--text-secondary)", fontSize: "0.625rem" }}>
                    R{String(match.rodada_id).padStart(2, "0")}
                  </span>
                  <span style={{ color: "var(--orange)", fontWeight: 600, fontSize: "0.75rem" }}>
                    {Number(match.pontuacao).toFixed(1)}
                  </span>
                </a>
              ))}
            </div>
          )}
        </div>
      );
    },
    [selectedPosition, expandedMatches, fetchMatchData, isMandante]
  );

  const handleStatusToggle = useCallback((statusId) => {
    setSelectedStatusIds((prev) =>
      prev.includes(statusId)
        ? prev.filter((id) => id !== statusId)
        : [...prev, statusId]
    );
  }, []);

  const topBarComponent = useMemo(
    () => (
      <div style={{ display: "flex", alignItems: "flex-start", gap: "1rem", flexWrap: "wrap", justifyContent: "space-between", width: "100%" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "1rem" }}>
          <RoundIntervalSlider
            min={1}
            max={statusData?.rodada_atual || 1}
            value={rodadaRange}
            onChange={setRodadaRange}
          />
          <MandoToggle value={isMandante} onChange={setIsMandante} />
          <ScoutSelect
            value={scout}
            onChange={(newScout) => {
              setScout(newScout);
              if (newScout) {
                setSortBy(newScout);
              } else {
                setSortBy("media_conquistada");
                setSortDirection("desc");
              }
            }}
            scouts={SCOUTS_BY_POSITION[selectedPosition]}
          />
          <button
            onClick={redefinirFiltros}
            style={{
              padding: "0.375rem 0.75rem",
              background: "var(--bg-card)",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius)",
              color: "var(--text-secondary)",
              cursor: "pointer",
              fontSize: "0.875rem",
            }}
          >
            Redefinir Filtros
          </button>
        </div>
        {filterOptions.status.length > 0 && (
          <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
            <span style={{ fontFamily: "var(--font-display)", fontSize: "0.75rem", color: "var(--text-muted)", textTransform: "uppercase" }}>Status:</span>
            <div style={{ display: "flex", gap: "0.25rem" }}>
              {filterOptions.status.map((s) => (
                <button
                  key={s.id}
                  onClick={() => handleStatusToggle(s.id)}
                  style={{
                    padding: "0.25rem 0.5rem",
                    fontSize: "0.7rem",
                    fontFamily: "var(--font-display)",
                    background: selectedStatusIds.includes(s.id) ? "var(--orange)" : "var(--bg-card)",
                    color: selectedStatusIds.includes(s.id) ? "white" : "var(--text-secondary)",
                    border: "1px solid var(--border)",
                    borderRadius: "var(--radius-sm)",
                    cursor: "pointer",
                  }}
                >
                  {s.nome}
                </button>
              ))}
            </div>
          </div>
        )}
      </div>
    ),
    [rodadaRange, isMandante, statusData?.rodada_atual, redefinirFiltros, filterOptions.status, selectedStatusIds, handleStatusToggle, scout, selectedPosition]
  );

  const currentPosition = filterOptions.posicoes.find((p) => p.id === selectedPosition);

  const positionTabsComponent = useMemo(
    () => (
      <div
        style={{
          display: "flex",
          gap: "0.5rem",
          marginBottom: "1rem",
          borderBottom: "1px solid var(--border)",
          paddingBottom: "0.5rem",
        }}
      >
        {filterOptions.posicoes.map((pos) => (
          <button
            key={pos.id}
            onClick={() => setSelectedPosition(pos.id)}
            title={pos.nome}
            style={{
              padding: "0.5rem 1rem",
              background:
                selectedPosition === pos.id ? "var(--orange)" : "transparent",
              color:
                selectedPosition === pos.id
                  ? "white"
                  : "var(--text-secondary)",
              border: "none",
              borderRadius: "var(--radius-sm)",
              cursor: "pointer",
              fontFamily: "var(--font-display)",
              fontSize: "0.875rem",
              fontWeight: 600,
              transition: "all var(--transition)",
            }}
          >
            {pos.abreviacao?.toUpperCase() || pos.nome.substring(0, 3).toUpperCase()}
          </button>
        ))}
      </div>
    ),
    [selectedPosition, filterOptions.posicoes]
  );

  const extraParams = useMemo(
    () => {
      const params = {
        rodada_min: rodadaRange.min,
        rodada_max: rodadaRange.max,
        is_mandante: isMandante,
        posicao_id: selectedPosition,
      };
      if (selectedStatusIds.length > 0) {
        params.status_ids = selectedStatusIds.join(",");
      }
      if (scout) {
        params.scout = scout;
      }
      return params;
    },
    [rodadaRange, isMandante, selectedPosition, selectedStatusIds, scout]
  );

  const POSICAO_LABELS = {
    gol: "Goleiro",
    lat: "Lateral",
    zag: "Zagueiro",
    mei: "Meio-Campo",
    ata: "Atacante",
    tec: "Técnico",
  };

  const subtitleMando = isMandante === "mandante" ? "mandante" : isMandante === "visitante" ? "visitante" : "de forma geral";
  const subtitlePosicao = currentPosition?.abreviacao ? POSICAO_LABELS[currentPosition.abreviacao] || currentPosition.abreviacao : "";

  return (
    <div>
      {positionTabsComponent}
      <TableView
        title="Pontos Conquistados"
        subtitle={`Pontos Conquistados, em média, por um atleta da posição ${subtitlePosicao} atuando como ${subtitleMando}`}
        endpoint="pontos-conquistados-unified"
        columns={columns}
        filterComponent={topBarComponent}
        extraParams={extraParams}
        expandable={true}
        expandedContent={renderExpandedContent}
        expandedRows={expandedRows}
        onExpandedRowsChange={handleExpandedRowsChange}
        hideCount={true}
        hideUpdate={true}
        hideTimestamps={true}
        sortBy={sortBy}
        sortDirection={sortDirection}
        onSortChange={(col, dir) => {
          setSortBy(col);
          setSortDirection(dir);
        }}
      />
    </div>
  );
}

export default PontosConquistadosUnified;

import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import TableView from "../components/TableView";
import RoundIntervalSlider from "../components/RoundIntervalSlider";
import MandoToggle from "../components/MandoToggle";

const SCOUTS_BY_POSITION = {
  1: ["SG", "DP", "DE", "GS", "CA", "CV", "FC", "GC", "PC"],
  2: ["G", "A", "FT", "FD", "FF", "FS", "PS", "V", "I", "PP", "DS", "SG", "DE", "CA", "CV", "FC", "GC", "GS", "PC"],
  3: ["G", "A", "FT", "FD", "FF", "FS", "PS", "V", "I", "PP", "DS", "SG", "DE", "CA", "CV", "FC", "GC", "GS", "PC"],
  4: ["G", "A", "FT", "FD", "FF", "FS", "PS", "V", "I", "PP", "DS", "SG", "DE", "CA", "CV", "FC", "GC", "GS", "PC"],
  5: ["G", "A", "FT", "FD", "FF", "FS", "PS", "V", "I", "PP", "DS", "SG", "DE", "CA", "CV", "FC", "GC", "GS", "PC"],
  6: ["V"],
};

function PontosCedidosUnified() {
  const [statusData, setStatusData] = useState(null);
  const [rodadaRange, setRodadaRange] = useState({ min: 1, max: 1 });
  const [isMandante, setIsMandante] = useState("geral");
  const [selectedPosition, setSelectedPosition] = useState(1);
  const [filterOptions, setFilterOptions] = useState({ clubes: {}, posicoes: [] });
  const [sortBy, setSortBy] = useState("media_cedida");
  const [sortDirection, setSortDirection] = useState("desc");

  const urlRestored = useRef(false);

  const ROUTE = "/pontos-cedidos";

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
    urlRestored.current = true;
  }, []);

  useEffect(() => {
    if (!urlRestored.current) return;

    const params = new URLSearchParams();
    if (rodadaRange.min !== 1) params.set("rodada_min", rodadaRange.min);
    if (rodadaRange.max !== (statusData?.rodada_atual ?? 1)) params.set("rodada_max", rodadaRange.max);
    if (isMandante !== "geral") params.set("is_mandante", isMandante);
    if (selectedPosition !== 1) params.set("posicao_id", selectedPosition);
    if (sortBy !== "media_cedida") params.set("sort_by", sortBy);
    if (sortDirection !== "desc") params.set("sort_direction", sortDirection);

    const queryString = params.toString();
    sessionStorage.setItem(ROUTE, queryString);

    const newUrl = queryString ? `${window.location.pathname}?${queryString}` : window.location.pathname;
    window.history.replaceState({}, "", newUrl);
  }, [rodadaRange, isMandante, selectedPosition, statusData, sortBy, sortDirection]);

  const redefinirFiltros = useCallback(() => {
    setRodadaRange({ min: 1, max: statusData?.rodada_atual || 1 });
    setIsMandante("geral");
    setSelectedPosition(1);
    setSortBy("media_cedida");
    setSortDirection("desc");
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
    () => [
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
        key: "media_cedida",
        label: "Média Cedida",
        sortable: true,
        renderCell: (row) => {
          const val = row.media_cedida;
          if (val == null) return "-";
          return Number(val).toFixed(2);
        },
      },
      {
        key: "media_cedida_basica",
        label: "Média Cedida Básica",
        sortable: true,
        renderCell: (row) => {
          const val = row.media_cedida_basica;
          return val != null ? Number(val).toFixed(2) : "-";
        },
      },
      { key: "total_jogos", label: "Total de Jogos", sortable: true },
    ],
    [getClubeEscudo, getClubeName]
  );

  const expandedContent = useCallback(
    (row) => {
      const scouts = row.scouts || {};
      const positionScouts = SCOUTS_BY_POSITION[selectedPosition] || [];
      const scoutEntries = Object.entries(scouts).filter(
        ([key, value]) =>
          value !== 0 &&
          value != null &&
          positionScouts.includes(key)
      );

      if (scoutEntries.length === 0) {
        return (
          <div style={{ padding: "1rem", color: "var(--text-muted)" }}>
            Nenhum scout registrado
          </div>
        );
      }

      return (
        <div style={{ padding: "1rem" }}>
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
            Média Cedida por Scout
          </div>
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: "0.5rem",
            }}
          >
            {scoutEntries.map(([key, value]) => (
              <span
                key={key}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: "0.25rem",
                  padding: "0.25rem 0.5rem",
                  background: "var(--bg-card)",
                  borderRadius: "var(--radius-sm)",
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.75rem",
                  color: "var(--text-primary)",
                }}
              >
                <span style={{ fontWeight: 600 }}>{key}</span>
                <span style={{ color: "var(--orange)" }}>{Number(value).toFixed(2)}</span>
              </span>
            ))}
          </div>
        </div>
      );
    },
    [selectedPosition]
  );

  const topBarComponent = useMemo(
    () => (
      <div style={{ display: "flex", alignItems: "center", gap: "1rem" }}>
        <RoundIntervalSlider
          min={1}
          max={statusData?.rodada_atual || 1}
          value={rodadaRange}
          onChange={setRodadaRange}
        />
        <MandoToggle value={isMandante} onChange={setIsMandante} />
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
    ),
    [rodadaRange, isMandante, statusData?.rodada_atual, redefinirFiltros]
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
    () => ({
      rodada_min: rodadaRange.min,
      rodada_max: rodadaRange.max,
      is_mandante: isMandante,
      posicao_id: selectedPosition,
    }),
    [rodadaRange, isMandante, selectedPosition]
  );

  return (
    <div>
      {positionTabsComponent}
      <TableView
        title={`Pontos Cedidos - ${currentPosition?.nome || ""}`}
        endpoint="pontos-cedidos-unified"
        columns={columns}
        filterComponent={topBarComponent}
        extraParams={extraParams}
        expandable={true}
        expandedContent={expandedContent}
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

export default PontosCedidosUnified;
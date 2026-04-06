import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import TableView from "../components/TableView";
import FilterSidebar from "../components/FilterSidebar";
import RoundIntervalSlider from "../components/RoundIntervalSlider";
import MandoToggle from "../components/MandoToggle";

const STATUS_COLORS = {
  green: "#22c55e",
  yellow: "#eab308",
  red: "#ef4444",
};

const DEFAULT_FILTERS = {
  search: "",
  clube_ids: [],
  posicao_ids: [],
  status_ids: [],
  preco_min: 0,
  preco_max: 30,
  options: { clubes: [], posicoes: [], status: [] },
};

function AtletasUnified() {
  const [statusData, setStatusData] = useState(null);
  const [rodadaRange, setRodadaRange] = useState({ min: 1, max: 1 });
  const [isMandante, setIsMandante] = useState("geral");
  const [filters, setFilters] = useState(DEFAULT_FILTERS);
  const [sortBy, setSortBy] = useState("media");
  const [sortDirection, setSortDirection] = useState("desc");

  const urlRestored = useRef(false);

  const ROUTE = "/atletas";

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
    if (params.get("search")) urlState.search = params.get("search");
    if (params.get("clube_ids")) urlState.clube_ids = params.get("clube_ids").split(",").map(Number);
    if (params.get("posicao_ids")) urlState.posicao_ids = params.get("posicao_ids").split(",").map(Number);
    if (params.get("status_ids")) urlState.status_ids = params.get("status_ids").split(",").map(Number);
    if (params.get("preco_min")) urlState.preco_min = parseInt(params.get("preco_min"), 10);
    if (params.get("preco_max")) urlState.preco_max = parseInt(params.get("preco_max"), 10);
    if (params.get("sort_by")) urlState.sort_by = params.get("sort_by");
    if (params.get("sort_direction")) urlState.sort_direction = params.get("sort_direction");

    if (Object.keys(urlState).length === 0) {
      const saved = sessionStorage.getItem(ROUTE);
      if (saved) {
        params = new URLSearchParams(saved);
        if (params.get("rodada_min")) urlState.rodada_min = parseInt(params.get("rodada_min"), 10);
        if (params.get("rodada_max")) urlState.rodada_max = parseInt(params.get("rodada_max"), 10);
        if (params.get("is_mandante")) urlState.is_mandante = params.get("is_mandante");
        if (params.get("search")) urlState.search = params.get("search");
        if (params.get("clube_ids")) urlState.clube_ids = params.get("clube_ids").split(",").map(Number);
        if (params.get("posicao_ids")) urlState.posicao_ids = params.get("posicao_ids").split(",").map(Number);
        if (params.get("status_ids")) urlState.status_ids = params.get("status_ids").split(",").map(Number);
        if (params.get("preco_min")) urlState.preco_min = parseInt(params.get("preco_min"), 10);
        if (params.get("preco_max")) urlState.preco_max = parseInt(params.get("preco_max"), 10);
        if (params.get("sort_by")) urlState.sort_by = params.get("sort_by");
        if (params.get("sort_direction")) urlState.sort_direction = params.get("sort_direction");
      }
    }

    if (Object.keys(urlState).length > 0) {
      setRodadaRange({ min: urlState.rodada_min ?? 1, max: urlState.rodada_max ?? 1 });
      setIsMandante(urlState.is_mandante ?? "geral");
      setFilters((prev) => ({
        ...prev,
        search: urlState.search ?? "",
        clube_ids: urlState.clube_ids ?? [],
        posicao_ids: urlState.posicao_ids ?? [],
        status_ids: urlState.status_ids ?? [],
        preco_min: urlState.preco_min ?? 0,
        preco_max: urlState.preco_max ?? 30,
      }));
      if (urlState.sort_by) setSortBy(urlState.sort_by);
      if (urlState.sort_direction) setSortDirection(urlState.sort_direction);
    }
    urlRestored.current = true;
  }, []);

  useEffect(() => {
    if (!urlRestored.current) return;

    const params = new URLSearchParams();
    if (rodadaRange.min !== 1) params.set("rodada_min", rodadaRange.min);
    if (rodadaRange.max !== (statusData?.rodada_atual ?? 1)) params.set("rodada_max", rodadaRange.max);
    if (isMandante !== "geral") params.set("is_mandante", isMandante);
    if (filters.search) params.set("search", filters.search);
    if (filters.clube_ids?.length) params.set("clube_ids", filters.clube_ids.join(","));
    if (filters.posicao_ids?.length) params.set("posicao_ids", filters.posicao_ids.join(","));
    if (filters.status_ids?.length) params.set("status_ids", filters.status_ids.join(","));
    if (filters.preco_min !== 0) params.set("preco_min", filters.preco_min);
    if (filters.preco_max !== 30) params.set("preco_max", filters.preco_max);
    if (sortBy !== "media") params.set("sort_by", sortBy);
    if (sortDirection !== "desc") params.set("sort_direction", sortDirection);

    const queryString = params.toString();
    sessionStorage.setItem(ROUTE, queryString);

    const newUrl = queryString ? `${window.location.pathname}?${queryString}` : window.location.pathname;
    window.history.replaceState({}, "", newUrl);
  }, [rodadaRange, isMandante, filters, statusData, sortBy, sortDirection]);

  const redefinirFiltros = useCallback(() => {
    setRodadaRange({ min: 1, max: statusData?.rodada_atual || 1 });
    setIsMandante("geral");
    setFilters((prev) => ({
      ...DEFAULT_FILTERS,
      options: prev.options,
    }));
    setSortBy("media");
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
        const sortedClubes = (data.clubes || []).sort((a, b) =>
          (a.nome_fantasia || "").localeCompare(b.nome_fantasia || ""),
        );
        setFilters((prev) => ({
          ...prev,
          options: {
            clubes: sortedClubes,
            posicoes: data.posicoes || [],
            status: data.status || [],
          },
        }));
      }
    } catch (err) {
      console.error("Failed to fetch filter options:", err);
    }
  };

  const handleFiltersChange = useCallback((newFilters) => {
    setFilters((prev) => ({ ...prev, ...newFilters }));
  }, []);

  const columns = useMemo(
    () => [
      { key: "apelido", label: "Atleta", sortable: true },
      {
        key: "clube_escudo",
        label: "Clube",
        sortable: false,
        renderCell: (row) => (
          <img
            src={row.clube_escudo}
            alt="clube"
            style={{ width: "24px", height: "24px", objectFit: "contain" }}
            onError={(e) => {
              e.target.style.display = "none";
            }}
          />
        ),
      },
      { key: "posicao_abreviacao", label: "Posição", sortable: true },
      {
        key: "status_nome",
        label: "Status",
        sortable: true,
        renderCell: (row) => (
          <span
            style={{
              color: STATUS_COLORS[row.status_cor] || STATUS_COLORS.red,
              fontWeight: 500,
            }}
          >
            {row.status_nome}
          </span>
        ),
      },
      {
        key: "preco",
        label: "Preço",
        sortable: true,
        renderCell: (row) => String(row.preco || ""),
      },
      { key: "media", label: "Média", sortable: true },
      { key: "media_basica", label: "Média Básica", sortable: true },
      { key: "total_jogos", label: "Total de Jogos", sortable: true },
      {
        key: "proximo_jogo",
        label: "Próximo Jogo",
        sortable: false,
        renderCell: (row) => {
          const jogo = row.proximo_jogo;
          if (!jogo || Object.keys(jogo).length === 0) {
            return <span style={{ color: "var(--text-muted)" }}>-</span>;
          }
          const hasMandante = Boolean(jogo.mandante_escudo);
          const hasVisitante = Boolean(jogo.visitante_escudo);
          if (!hasMandante && !hasVisitante) {
            return (
              <span style={{ color: "var(--text-muted)", fontSize: "0.75rem" }}>
                -
              </span>
            );
          }
          return (
            <div
              style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}
            >
              {hasMandante ? (
                <img
                  src={jogo.mandante_escudo}
                  alt="mandante"
                  style={{
                    width: "20px",
                    height: "20px",
                    objectFit: "contain",
                  }}
                  onError={(e) => {
                    e.target.style.display = "none";
                  }}
                />
              ) : (
                <span style={{ width: "20px", height: "20px" }} />
              )}
              <span style={{ color: "var(--text-muted)", fontSize: "0.75rem" }}>
                x
              </span>
              {hasVisitante ? (
                <img
                  src={jogo.visitante_escudo}
                  alt="visitante"
                  style={{
                    width: "20px",
                    height: "20px",
                    objectFit: "contain",
                  }}
                  onError={(e) => {
                    e.target.style.display = "none";
                  }}
                />
              ) : (
                <span style={{ width: "20px", height: "20px" }} />
              )}
            </div>
          );
        },
      },
    ],
    [],
  );

  const expandedContent = useCallback((row) => {
    const scouts = row.scouts || {};
    const scoutEntries = Object.entries(scouts).filter(
      ([_, value]) => value > 0,
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
          Scouts
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
              <span style={{ color: "var(--orange)" }}>{value}</span>
            </span>
          ))}
        </div>
      </div>
    );
  }, []);

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
    [rodadaRange, isMandante, statusData?.rodada_atual, redefinirFiltros],
  );

  const extraParams = useMemo(() => {
    const params = {
      rodada_min: rodadaRange.min,
      rodada_max: rodadaRange.max,
      is_mandante: isMandante,
    };

    if (filters.search) params.search = filters.search;
    if (filters.clube_ids && filters.clube_ids.length > 0)
      params.clube_ids = filters.clube_ids.join(",");
    if (filters.posicao_ids && filters.posicao_ids.length > 0)
      params.posicao_ids = filters.posicao_ids.join(",");
    if (filters.status_ids && filters.status_ids.length > 0)
      params.status_ids = filters.status_ids.join(",");
    if (filters.preco_min !== 0) params.preco_min = filters.preco_min;
    if (filters.preco_max !== 30) params.preco_max = filters.preco_max;

    return params;
  }, [rodadaRange, isMandante, filters]);

  return (
    <div style={{ display: "flex", gap: "1.5rem" }}>
      <FilterSidebar filters={filters} onFiltersChange={handleFiltersChange} />
      <div style={{ flex: 1 }}>
        <TableView
          title="Atletas"
          endpoint="atletas-unified"
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
    </div>
  );
}

export default AtletasUnified;

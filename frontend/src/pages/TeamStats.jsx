import { useEffect, useMemo, useRef, useState } from "react";
import MandoToggle from "../components/MandoToggle";
import RoundIntervalSlider from "../components/RoundIntervalSlider";
import "./TeamStats.css";

const ROUTE = "/times";

const GROUPS = [
  { key: "overview", label: "Visão geral", number: "01" },
  { key: "attack", label: "Ataque", number: "02" },
  { key: "defense", label: "Defesa", number: "03" },
];

function readInitialState() {
  let params = new URLSearchParams(window.location.search);
  if (![...params.keys()].length) {
    params = new URLSearchParams(sessionStorage.getItem(ROUTE) || "");
  }
  return {
    min: Number(params.get("rodada_min")) || 1,
    max: Number(params.get("rodada_max")) || 1,
    hasMax: params.has("rodada_max"),
    venue: params.get("is_mandante") || "geral",
    group: GROUPS.some(({ key }) => key === params.get("grupo"))
      ? params.get("grupo")
      : "overview",
  };
}

function formatValue(metric, value) {
  if (value == null) return "—";
  const precision = Number(metric.precision ?? 1);
  const formatted = Number(value).toLocaleString("pt-BR", {
    minimumFractionDigits: precision,
    maximumFractionDigits: precision,
  });
  return `${formatted}${metric.suffix || ""}`;
}

function RankingCard({ metric, expanded, onToggle }) {
  const visibleTeams = expanded ? metric.teams : metric.teams.slice(0, 5);
  const leader = metric.teams[0];

  return (
    <article className="team-ranking-card">
      <button className="team-ranking-heading" type="button" onClick={onToggle}>
        <span>
          <small>{metric.group === "attack" ? "ATAQUE" : metric.group === "defense" ? "DEFESA" : "PERFORMANCE"}</small>
          <strong>{metric.title}</strong>
        </span>
        <span className={`team-ranking-arrow ${expanded ? "is-open" : ""}`} aria-hidden="true">↗</span>
      </button>

      {visibleTeams.length ? (
        <ol className="team-ranking-list">
          {visibleTeams.map((team, index) => (
            <li key={team.team_id} className={index === 0 ? "is-leader" : ""}>
              <span className="team-rank">{String(team.rank).padStart(2, "0")}</span>
              <img
                src={team.logo}
                alt=""
                loading="lazy"
                onError={(event) => { event.currentTarget.style.visibility = "hidden"; }}
              />
              <span className="team-ranking-name">
                <strong>{team.team_name}</strong>
                <small>{team.games} {team.games === 1 ? "jogo" : "jogos"}</small>
              </span>
              <b className={index === 0 ? "leader-value" : ""}>
                {formatValue(metric, team.value)}
              </b>
            </li>
          ))}
        </ol>
      ) : (
        <div className="team-ranking-empty">Sem jogos neste recorte.</div>
      )}

      {metric.teams.length > 5 && (
        <button className="team-ranking-more" type="button" onClick={onToggle}>
          {expanded ? "Recolher ranking" : `Ver todos os ${metric.teams.length} times`}
          <span>{expanded ? "↑" : "↓"}</span>
        </button>
      )}

      {leader && <div className="team-card-watermark">{leader.team_name.slice(0, 3).toUpperCase()}</div>}
    </article>
  );
}

function TeamStats() {
  const initial = useMemo(readInitialState, []);
  const [statusData, setStatusData] = useState(null);
  const [roundRange, setRoundRange] = useState({ min: initial.min, max: initial.max });
  const [venue, setVenue] = useState(initial.venue);
  const [group, setGroup] = useState(initial.group);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [expanded, setExpanded] = useState(() => new Set());
  const requestCount = useRef(0);

  useEffect(() => {
    let active = true;
    fetch("/api/tables/status")
      .then((response) => {
        if (!response.ok) throw new Error();
        return response.json();
      })
      .then((status) => {
        if (!active) return;
        const currentRound = Math.max(Number(status.rodada_atual) || 1, 1);
        setStatusData({ ...status, rodada_atual: currentRound });
        setRoundRange((current) => ({
          min: Math.min(current.min, Math.max(currentRound - 1, 1)),
          max: initial.hasMax ? Math.min(current.max, currentRound) : currentRound,
        }));
      })
      .catch(() => {
        if (active) setStatusData({ rodada_atual: Math.max(initial.max, 1) });
      });
    return () => { active = false; };
  }, [initial.hasMax, initial.max]);

  useEffect(() => {
    if (!statusData) return undefined;

    const urlParams = new URLSearchParams();
    if (roundRange.min !== 1) urlParams.set("rodada_min", roundRange.min);
    if (roundRange.max !== statusData.rodada_atual) urlParams.set("rodada_max", roundRange.max);
    if (venue !== "geral") urlParams.set("is_mandante", venue);
    if (group !== "overview") urlParams.set("grupo", group);
    const queryString = urlParams.toString();
    sessionStorage.setItem(ROUTE, queryString);
    window.history.replaceState({}, "", `${ROUTE}${queryString ? `?${queryString}` : ""}`);
  }, [group, roundRange, statusData, venue]);

  useEffect(() => {
    if (!statusData) return undefined;

    const controller = new AbortController();
    const requestId = ++requestCount.current;
    const timer = window.setTimeout(async () => {
      setLoading(true);
      setError("");
      try {
        const apiParams = new URLSearchParams({
          rodada_min: String(roundRange.min),
          rodada_max: String(roundRange.max),
          is_mandante: venue,
        });
        const response = await fetch(`/api/team-stats?${apiParams}`, { signal: controller.signal });
        if (!response.ok) {
          const payload = await response.json().catch(() => ({}));
          throw new Error(payload.detail || "Não foi possível carregar as estatísticas.");
        }
        const payload = await response.json();
        if (requestCount.current === requestId) setData(payload);
      } catch (requestError) {
        if (requestError.name !== "AbortError" && requestCount.current === requestId) {
          setError(requestError.message);
        }
      } finally {
        if (!controller.signal.aborted && requestCount.current === requestId) setLoading(false);
      }
    }, 260);

    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [roundRange, statusData, venue]);

  const metrics = data?.metrics?.filter((metric) => metric.group === group) || [];
  const activeGroup = GROUPS.find((item) => item.key === group);
  const updatedAt = data?.updated_at
    ? new Intl.DateTimeFormat("pt-BR", { dateStyle: "short", timeStyle: "short" }).format(new Date(data.updated_at))
    : "sincronizando";

  const toggleMetric = (key) => {
    setExpanded((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const resetFilters = () => {
    setRoundRange({ min: 1, max: statusData?.rodada_atual || 1 });
    setVenue("geral");
    setGroup("overview");
    sessionStorage.removeItem(ROUTE);
  };

  return (
    <div className="team-stats-page">
      <header className="team-stats-hero">
        <div className="team-hero-copy">
          <span className="team-kicker"><i /> BRASILEIRÃO · PERFORMANCE</span>
          <h1><span>Radar de</span><em>times</em></h1>
          <p>O Brasileirão, reconstruído jogo a jogo para comparar performance real em qualquer recorte.</p>
        </div>
        <div className="team-hero-aside">
          <span>BASE ATIVA</span>
          <strong>{data?.season || "—"}</strong>
          <div><b>{data?.sample_matches ?? "—"}</b><small>partidas no recorte</small></div>
          <p>Dados jogo a jogo<br />Atualizado {updatedAt}</p>
        </div>
        <div className="team-hero-grid" aria-hidden="true" />
      </header>

      <aside className="team-stats-filters" aria-label="Filtros das estatísticas de times">
        <div className="team-filter-recut">
          <span>RECORTE</span>
          <b>R{roundRange.min}—R{roundRange.max}</b>
        </div>
        <RoundIntervalSlider
          min={1}
          max={statusData?.rodada_atual || Math.max(roundRange.max, 1)}
          value={roundRange}
          onChange={setRoundRange}
        />
        <MandoToggle value={venue} onChange={setVenue} />
        <button className="team-filter-reset" type="button" onClick={resetFilters}>Redefinir</button>
      </aside>

      <section className="team-stats-section">
        <div className="team-section-heading">
          <div>
            <span>{activeGroup?.number}</span>
            <div><small>RANKINGS DO RECORTE</small><h2>{activeGroup?.label}</h2></div>
          </div>
          <p>Médias por partida, salvo quando indicado. O mando corresponde ao time analisado.</p>
        </div>

        <div className="team-group-tabs" role="tablist" aria-label="Categorias de estatísticas">
          {GROUPS.map((item) => (
            <button
              key={item.key}
              type="button"
              role="tab"
              aria-selected={group === item.key}
              className={group === item.key ? "is-active" : ""}
              onClick={() => setGroup(item.key)}
            >
              <span>{item.number}</span>{item.label}
            </button>
          ))}
        </div>

        {loading && !data ? (
          <div className="team-stats-loading">
            <div><span /><span /><span /></div>
            <h2>Construindo a base jogo a jogo</h2>
            <p>Na primeira abertura, buscamos apenas as partidas concluídas. Depois disso, a atualização é incremental.</p>
          </div>
        ) : error && !data ? (
          <div className="team-stats-error"><b>!</b><h2>Base temporariamente indisponível</h2><p>{error}</p></div>
        ) : (
          <>
            {error && <div className="team-inline-error">{error} Exibindo o último recorte carregado.</div>}
            <div className={`team-ranking-grid ${loading ? "is-refreshing" : ""}`}>
              {metrics.map((metric) => (
                <RankingCard
                  key={metric.key}
                  metric={metric}
                  expanded={expanded.has(metric.key)}
                  onToggle={() => toggleMetric(metric.key)}
                />
              ))}
            </div>
          </>
        )}
      </section>

      <footer className="team-stats-footnote">
        <span>OPTA ≠ CARTOLA FC</span>
        <p>Estatísticas de jogo do Opta permanecem em uma base própria e nunca são somadas aos scouts ou pontos do Cartola.</p>
      </footer>
    </div>
  );
}

export default TeamStats;

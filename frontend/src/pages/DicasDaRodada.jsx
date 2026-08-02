import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import RoundIntervalSlider from "../components/RoundIntervalSlider";
import "./DicasDaRodada.css";

const CONFIDENCE_CLASS = {
  Alta: "is-high",
  Média: "is-medium",
  Baixa: "is-low",
};

const TARGETS_PER_PAGE = 6;

function paginationItems(current, total) {
  if (total <= 7) return Array.from({ length: total }, (_, index) => index + 1);
  const pages = new Set([1, total, current - 1, current, current + 1]);
  const ordered = [...pages].filter((page) => page >= 1 && page <= total).sort((a, b) => a - b);
  const result = [];
  ordered.forEach((page, index) => {
    if (index > 0 && page - ordered[index - 1] > 1) result.push(`gap-${page}`);
    result.push(page);
  });
  return result;
}

function getPhoto(photo) {
  return photo?.replace("FORMATO", "140x140") || "";
}

function formatNumber(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toLocaleString("pt-BR", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatKickoff(value) {
  if (!value) return { date: "Data a confirmar", time: "—" };
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return { date: value, time: "—" };
  return {
    date: date.toLocaleDateString("pt-BR", { weekday: "short", day: "2-digit", month: "short" }),
    time: date.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" }),
  };
}

function TeamBadge({ team, size = "normal" }) {
  return (
    <div className={`match-team-badge match-team-badge--${size}`}>
      {team?.badge ? <img src={team.badge} alt="" /> : <span>{team?.name?.slice(0, 2) || "?"}</span>}
    </div>
  );
}

function MatchRail({ matches, activeId, onSelect, disabled }) {
  return (
    <div className="match-rail" aria-label="Jogos da próxima rodada">
      {matches.map((match, index) => {
        const active = String(match.id) === String(activeId);
        return (
          <button
            key={match.id || `${match.home.id}-${match.away.id}`}
            className={`match-ticket ${active ? "is-active" : ""}`}
            onClick={() => onSelect(match.id)}
            disabled={disabled || active}
          >
            <span className="match-ticket-index">{String(index + 1).padStart(2, "0")}</span>
            <div><TeamBadge team={match.home} size="tiny" /><b>{match.home.name}</b></div>
            <i>×</i>
            <div><TeamBadge team={match.away} size="tiny" /><b>{match.away.name}</b></div>
          </button>
        );
      })}
    </div>
  );
}

function MetricComparison({ definition, home, away }) {
  const homeValue = home?.metrics?.[definition.key];
  const awayValue = away?.metrics?.[definition.key];
  const validHome = homeValue !== null && homeValue !== undefined;
  const validAway = awayValue !== null && awayValue !== undefined;
  const max = Math.max(Math.abs(Number(homeValue) || 0), Math.abs(Number(awayValue) || 0), 1);
  const lowerWins = definition.lower_is_better;
  const homeWins = validHome && validAway && (lowerWins ? homeValue < awayValue : homeValue > awayValue);
  const awayWins = validHome && validAway && (lowerWins ? awayValue < homeValue : awayValue > homeValue);

  return (
    <article className="metric-comparison">
      <div className="metric-values">
        <strong className={homeWins ? "wins" : ""}>{formatNumber(homeValue, definition.precision ?? 1)}{homeValue !== null && homeValue !== undefined ? definition.suffix || "" : ""}</strong>
        <span>{definition.label}</span>
        <strong className={awayWins ? "wins" : ""}>{formatNumber(awayValue, definition.precision ?? 1)}{awayValue !== null && awayValue !== undefined ? definition.suffix || "" : ""}</strong>
      </div>
      <div className="metric-bars" aria-hidden="true">
        <i><b style={{ width: `${(Math.abs(Number(homeValue) || 0) / max) * 100}%` }} /></i>
        <i><b style={{ width: `${(Math.abs(Number(awayValue) || 0) / max) * 100}%` }} /></i>
      </div>
    </article>
  );
}

function TargetCard({ target, contextualVenue, rodadaRange }) {
  const average = contextualVenue && target.media_no_mando != null
    ? target.media_no_mando
    : target.media;
  const conceded = target.adversario_cede?.media_cedida;
  const topScout = target.overlap_scouts?.[0];

  return (
    <a className="target-card" href={`/atletas/${target.atleta_id}`}>
      <div className="target-photo">
        {target.foto ? <img src={getPhoto(target.foto)} alt="" /> : <span>{target.apelido?.slice(0, 1)}</span>}
      </div>
      <div className="target-copy">
        <div className="target-meta"><span>{target.posicao}</span><i>•</i><span>{target.clube_nome}</span><i>•</i><span className="probable-mark">PROVÁVEL</span></div>
        <h3>{target.apelido}</h3>
        <p>
          {topScout
            ? `${topScout.scout_nome}: ${formatNumber(topScout.opponent_avg)} cedida/jogo`
            : target.resumo_humano || "Leitura baseada em forma e duelo por posição."}
        </p>
      </div>
      <div className="target-numbers">
        <div><span>R{rodadaRange.min}–R{rodadaRange.max}{contextualVenue ? " · MANDO" : ""}</span><strong>{formatNumber(average)}</strong></div>
        <div><span>RIVAL CEDE</span><strong>{formatNumber(conceded)}</strong></div>
        <div className="target-score"><span>EDGE</span><strong>{formatNumber(target.score, 1)}</strong></div>
      </div>
      <span className={`confidence-pill ${CONFIDENCE_CLASS[target.confidence] || ""}`}>{target.confidence || "Monitorar"}</span>
      <span className="target-arrow">↗</span>
    </a>
  );
}

function ConcededPanel({ side, attackingTeam, defendingTeam, contextualVenue }) {
  const data = side?.conceded || {};
  return (
    <div className="conceded-panel">
      <header>
        <TeamBadge team={attackingTeam} size="small" />
        <div><span>{attackingTeam?.name} ATACA · {defendingTeam?.name} DEFENDE</span><h3>Oportunidade para {attackingTeam?.name}</h3></div>
        <div className="conceded-score"><b>{formatNumber(data.points)}</b><small>pts / jogo</small></div>
      </header>
      <div className="conceded-kpis">
        <div><span>Pontuação básica</span><b>{formatNumber(data.basic_points)}</b></div>
        <div><span>Amostra</span><b>{data.games || 0} jogos</b></div>
        <div><span>Edge dos alvos</span><b>{formatNumber(side?.edge)}</b></div>
      </div>
      <div className="scout-list">
        {data.scouts?.length ? data.scouts.map((scout) => (
          <div key={scout.code} className={scout.average === 0 ? "is-zero" : ""}>
            <span className="scout-code">{scout.code}</span>
            <p><b>{scout.label}</b><small>{formatNumber(scout.average)} por jogo</small></p>
            <strong className={scout.points >= 0 ? "positive" : "negative"}>{scout.points > 0 ? "+" : ""}{formatNumber(scout.points)} pts</strong>
          </div>
        )) : <p className="duel-empty">Sem scouts suficientes neste recorte.</p>}
      </div>
      <div className="duel-targets">
        <span>PROVÁVEIS DE {attackingTeam?.name}</span>
        {side?.targets?.length ? side.targets.map((target) => (
          <a href={`/atletas/${target.atleta_id}`} key={target.atleta_id}>
            {target.foto ? <img src={getPhoto(target.foto)} alt="" /> : <i>{target.apelido?.slice(0, 1)}</i>}
            <div><b>{target.apelido}</b><small>{formatNumber(contextualVenue && target.media_no_mando != null ? target.media_no_mando : target.media)} pts</small></div>
            <strong>{formatNumber(target.score)}</strong>
          </a>
        )) : <p className="duel-empty">Nenhum atleta com amostra confiável.</p>}
      </div>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="match-analysis-loading">
      <div><span /><span /><span /><span /></div>
      <p>Montando os duelos da próxima rodada</p>
      <small>Cartola FC + Opta via FotMob</small>
    </div>
  );
}

function DicasDaRodada() {
  const initialParamsRef = useRef(new URLSearchParams(window.location.search));
  const initialMatch = Number(initialParamsRef.current.get("match")) || null;
  const initialMin = Number(initialParamsRef.current.get("rodada_min")) || null;
  const initialMax = Number(initialParamsRef.current.get("rodada_max")) || null;
  const initialContextualVenue = initialParamsRef.current.get("contextual_venue") !== "0";
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [switching, setSwitching] = useState(false);
  const [error, setError] = useState(null);
  const [rodadaRange, setRodadaRange] = useState(
    initialMin && initialMax ? { min: initialMin, max: initialMax } : null,
  );
  const [contextualVenue, setContextualVenue] = useState(initialContextualVenue);
  const [positionId, setPositionId] = useState(null);
  const [targetPage, setTargetPage] = useState(1);
  const [fotmobPage, setFotmobPage] = useState("overview");
  const cacheRef = useRef(new Map());
  const requestRef = useRef(null);
  const rangeTimerRef = useRef(null);
  const targetsSectionRef = useRef(null);
  const filtersRef = useRef({
    range: initialMin && initialMax ? { min: initialMin, max: initialMax } : null,
    contextualVenue: initialContextualVenue,
  });

  const loadMatch = useCallback(async (matchId = null, initial = false, explicitFilters = null) => {
    const activeFilters = explicitFilters || filtersRef.current;
    const range = activeFilters.range;
    const cacheKey = `${matchId || "first"}|${range?.min || "auto"}-${range?.max || "auto"}|${activeFilters.contextualVenue ? "venue" : "general"}`;
    const commitPayload = (payload) => {
      const responseRange = {
        min: payload.filters.rodada_min,
        max: payload.filters.rodada_max,
      };
      filtersRef.current = {
        range: responseRange,
        contextualVenue: payload.filters.contextual_venue,
      };
      setRodadaRange(responseRange);
      setContextualVenue(payload.filters.contextual_venue);
      setData(payload);
      setPositionId(null);
      setTargetPage(1);
      setFotmobPage("overview");
      if (payload.selected?.id) {
        const params = new URLSearchParams();
        params.set("match", payload.selected.id);
        params.set("rodada_min", responseRange.min);
        params.set("rodada_max", responseRange.max);
        if (!payload.filters.contextual_venue) params.set("contextual_venue", "0");
        window.history.replaceState({}, "", `${window.location.pathname}?${params.toString()}`);
      }
    };
    if (cacheRef.current.has(cacheKey)) {
      commitPayload(cacheRef.current.get(cacheKey));
      return;
    }
    requestRef.current?.abort();
    const controller = new AbortController();
    requestRef.current = controller;
    initial ? setLoading(true) : setSwitching(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (matchId) params.set("match_id", matchId);
      if (range) {
        params.set("rodada_min", range.min);
        params.set("rodada_max", range.max);
      }
      params.set("contextual_venue", String(activeFilters.contextualVenue));
      const response = await fetch(`/api/dicas-da-rodada/matches?${params.toString()}`, { signal: controller.signal });
      if (!response.ok) throw new Error("Não foi possível montar a análise desta partida.");
      const payload = await response.json();
      cacheRef.current.set(cacheKey, payload);
      commitPayload(payload);
    } catch (err) {
      if (err.name !== "AbortError") setError(err.message);
    } finally {
      if (!controller.signal.aborted) {
        setLoading(false);
        setSwitching(false);
      }
    }
  }, []);

  useEffect(() => {
    loadMatch(initialMatch, true, filtersRef.current);
    return () => {
      requestRef.current?.abort();
      window.clearTimeout(rangeTimerRef.current);
    };
  }, [initialMatch, loadMatch]);

  const changeRange = useCallback((nextRange) => {
    setRodadaRange(nextRange);
    filtersRef.current = { ...filtersRef.current, range: nextRange };
    window.clearTimeout(rangeTimerRef.current);
    rangeTimerRef.current = window.setTimeout(() => {
      loadMatch(data?.selected?.id || null, false, filtersRef.current);
    }, 220);
  }, [data?.selected?.id, loadMatch]);

  const changeVenueContext = useCallback(() => {
    const nextValue = !filtersRef.current.contextualVenue;
    setContextualVenue(nextValue);
    filtersRef.current = { ...filtersRef.current, contextualVenue: nextValue };
    loadMatch(data?.selected?.id || null, false, filtersRef.current);
  }, [data?.selected?.id, loadMatch]);

  const selected = data?.selected;
  const activeDuel = useMemo(() => {
    if (!selected?.duels?.length) return null;
    return selected.duels.find((duel) => duel.position_id === positionId) || selected.duels[0];
  }, [selected, positionId]);
  const homeForm = selected?.home?.form?.selected;
  const awayForm = selected?.away?.form?.selected;
  const fotmobGroups = useMemo(() => {
    const labels = { overview: "Visão geral", attack: "Ataque", defense: "Defesa" };
    return ["overview", "attack", "defense"].map((group) => ({
      group,
      label: labels[group],
      metrics: (data?.fotmob?.metrics || []).filter((metric) => metric.group === group),
    }));
  }, [data?.fotmob?.metrics]);
  const activeFotmobGroup = fotmobGroups.find((group) => group.group === fotmobPage) || fotmobGroups[0];
  const activeFotmobPage = Math.max(0, fotmobGroups.findIndex((group) => group.group === activeFotmobGroup.group));
  const kickoff = formatKickoff(selected?.kickoff);
  const targets = useMemo(() => {
    const list = selected?.top_targets || [];
    return [...list].sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
  }, [selected]);
  const targetPageCount = Math.max(1, Math.ceil(targets.length / TARGETS_PER_PAGE));
  const paginatedTargets = useMemo(() => {
    const start = (targetPage - 1) * TARGETS_PER_PAGE;
    return targets.slice(start, start + TARGETS_PER_PAGE);
  }, [targetPage, targets]);

  const changeTargetPage = useCallback((page) => {
    const nextPage = Math.max(1, Math.min(page, targetPageCount));
    setTargetPage(nextPage);
    window.requestAnimationFrame(() => {
      targetsSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }, [targetPageCount]);

  if (loading) return <LoadingState />;
  if (error && !data) {
    return <div className="match-analysis-error"><b>!</b><h1>Análise indisponível</h1><p>{error}</p><button onClick={() => loadMatch(initialMatch, true)}>Tentar novamente</button></div>;
  }
  if (!selected) {
    return <div className="match-analysis-error"><b>—</b><h1>Rodada ainda sem confrontos</h1><p>Assim que a tabela do Cartola liberar, os jogos aparecerão aqui.</p></div>;
  }

  return (
    <div className={`match-analysis-page ${switching ? "is-switching" : ""}`}>
      <header className="match-analysis-header">
        <h1>Dicas da Rodada</h1>
      </header>

      <MatchRail matches={data.matches} activeId={selected.id} onSelect={(id) => loadMatch(id, false, filtersRef.current)} disabled={switching} />
      {error && <div className="match-inline-error">{error}</div>}

      <section className="selected-match-hero">
        <div className="hero-team hero-team--home"><TeamBadge team={selected.home} size="hero" /><div><span>MANDANTE</span><h2>{selected.home.name}</h2></div></div>
        <div className="hero-fixture">
          <span>{kickoff.date}</span>
          <strong><i>H</i><b>×</b><i>A</i></strong>
          <p>{kickoff.time} · {selected.venue || "Local a confirmar"}</p>
        </div>
        <div className="hero-team hero-team--away"><div><span>VISITANTE</span><h2>{selected.away.name}</h2></div><TeamBadge team={selected.away} size="hero" /></div>
        <div className="fixture-stamp">MATCH #{selected.id || "—"}</div>
      </section>

      <nav className="analysis-toolbar">
        <div><span>RECORTE DE ANÁLISE</span><b>R{rodadaRange.min} — R{rodadaRange.max}</b></div>
        <div className="match-round-slider">
          <RoundIntervalSlider
            min={1}
            max={data.round - 1}
            value={rodadaRange}
            onChange={changeRange}
          />
        </div>
        <button className={`venue-context ${contextualVenue ? "is-active" : ""}`} onClick={changeVenueContext} aria-pressed={contextualVenue}><i />Usar mando do confronto</button>
        <p><i /> Dados fechados até a rodada {data.round - 1}</p>
      </nav>

      <section className="analysis-section">
        <div className="section-heading">
          <div><span>01</span><div><small>OPTA · VIA FOTMOB</small><h2>Raio-X do confronto</h2></div></div>
          <p>Médias por partida. Verde marca a vantagem estatística no recorte selecionado.</p>
        </div>
        <div className="fotmob-board">
          <header><div><TeamBadge team={selected.home} size="small" /><b>{selected.home.name}</b><small>{homeForm?.games || 0} jogos</small></div><span>{contextualVenue ? "CASA / FORA" : `R${rodadaRange.min}–R${rodadaRange.max}`}</span><div><small>{awayForm?.games || 0} jogos</small><b>{selected.away.name}</b><TeamBadge team={selected.away} size="small" /></div></header>
          {selected.home.form?.available || selected.away.form?.available ? (
            <>
              <nav className="fotmob-pagination" aria-label="Páginas de estatísticas FotMob">
                {fotmobGroups.map((group, index) => (
                  <button
                    key={group.group}
                    className={group.group === activeFotmobGroup.group ? "is-active" : ""}
                    onClick={() => setFotmobPage(group.group)}
                    aria-current={group.group === activeFotmobGroup.group ? "page" : undefined}
                  >
                    <span>{String(index + 1).padStart(2, "0")}</span>
                    <b>{group.label}</b>
                    <small>{group.metrics.length} métricas</small>
                  </button>
                ))}
              </nav>
              <section className="fotmob-metric-page" key={activeFotmobGroup.group} aria-label={activeFotmobGroup.label}>
                <div className="metrics-grid">{activeFotmobGroup.metrics.map((metric) => <MetricComparison key={metric.key} definition={metric} home={homeForm} away={awayForm} />)}</div>
              </section>
            </>
          ) : <div className="fotmob-empty"><span>FM</span><div><b>Histórico Opta ainda não sincronizado</b><p>Os duelos Cartola continuam disponíveis abaixo; este bloco entra assim que a coleta do FotMob tiver partidas concluídas.</p></div></div>}
          <footer><span>FONTE: {data.fotmob.source}</span><span>PÁGINA {activeFotmobPage + 1} / {fotmobGroups.length}</span><span>ATUALIZAÇÃO: {data.fotmob.updated_at ? new Date(data.fotmob.updated_at).toLocaleString("pt-BR") : "aguardando coleta"}</span></footer>
        </div>
      </section>

      <section className="analysis-section targets-section" ref={targetsSectionRef}>
        <div className="section-heading">
          <div><span>02</span><div><small>CARTOLA FC · FORMA × CONCESSÃO</small><h2>Alvos que o duelo favorece</h2></div></div>
          <p>Somente atletas com status Provável. O edge cruza o recorte selecionado, mando e concessão do rival.</p>
        </div>
        <div className="targets-list">
          {targets.length ? paginatedTargets.map((target) => <TargetCard key={target.atleta_id} target={target} contextualVenue={contextualVenue} rodadaRange={rodadaRange} />) : <div className="section-empty">Nenhum atleta Provável atingiu amostra suficiente neste confronto.</div>}
        </div>
        {targets.length > TARGETS_PER_PAGE && (
          <nav className="targets-pagination" aria-label="Paginação dos atletas prováveis">
            <p><b>{(targetPage - 1) * TARGETS_PER_PAGE + 1}–{Math.min(targetPage * TARGETS_PER_PAGE, targets.length)}</b> de {targets.length} prováveis · ordenados por edge</p>
            <div>
              <button className="pagination-arrow" onClick={() => changeTargetPage(targetPage - 1)} disabled={targetPage === 1} aria-label="Página anterior">←</button>
              {paginationItems(targetPage, targetPageCount).map((item) => typeof item === "string" ? <span key={item}>…</span> : <button key={item} className={item === targetPage ? "is-active" : ""} onClick={() => changeTargetPage(item)} aria-current={item === targetPage ? "page" : undefined}>{String(item).padStart(2, "0")}</button>)}
              <button className="pagination-arrow" onClick={() => changeTargetPage(targetPage + 1)} disabled={targetPage === targetPageCount} aria-label="Próxima página">→</button>
            </div>
          </nav>
        )}
      </section>

      <section className="analysis-section">
        <div className="section-heading">
          <div><span>03</span><div><small>DUELOS POR SETOR</small><h2>Onde atacar cada defesa</h2></div></div>
          <p>Pontos e scouts cedidos mostram como cada posição costuma pontuar contra o rival.</p>
        </div>
        <div className="position-tabs" role="tablist">
          {selected.duels.map((duel) => <button role="tab" aria-selected={activeDuel?.position_id === duel.position_id} className={activeDuel?.position_id === duel.position_id ? "is-active" : ""} key={duel.position_id} onClick={() => setPositionId(duel.position_id)}><span>{String(duel.position_id).padStart(2, "0")}</span>{duel.position}</button>)}
        </div>
        {activeDuel && <div className="duel-grid"><ConcededPanel side={activeDuel.home} attackingTeam={selected.home} defendingTeam={selected.away} contextualVenue={contextualVenue} /><ConcededPanel side={activeDuel.away} attackingTeam={selected.away} defendingTeam={selected.home} contextualVenue={contextualVenue} /></div>}
      </section>

      <footer className="methodology-note">
        <span>COMO LER</span>
        <div>{data.methodology.map((note, index) => <p key={note}><b>{String(index + 1).padStart(2, "0")}</b>{note}</p>)}</div>
      </footer>
    </div>
  );
}

export default DicasDaRodada;

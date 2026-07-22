import { useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";
import MandoToggle from "../components/MandoToggle";
import PlayerPointsChart from "../components/PlayerPointsChart";
import PlayerShotMap from "../components/PlayerShotMap";
import RoundIntervalSlider from "../components/RoundIntervalSlider";
import "./PlayerView.css";

const GROUP_LABELS = {
  "Top stats": "Resumo",
  Attack: "Ataque",
  Defense: "Defesa",
  Duels: "Duelos",
  Passing: "Passe",
  Discipline: "Disciplina",
  Goalkeeping: "Goleiro",
};

const METRIC_LABELS = {
  "FotMob rating": "Nota FotMob",
  "Minutes played": "Minutos",
  Goals: "Gols",
  Assists: "Assistências",
  "Expected goals (xG)": "Gols esperados (xG)",
  "Expected goals on target (xGOT)": "xG no alvo (xGOT)",
  "Expected assists (xA)": "Assistências esperadas (xA)",
  "Total shots": "Finalizações",
  "Accurate passes": "Passes certos",
  "Chances created": "Chances criadas",
  "Shots on target": "Finalizações no alvo",
  Touches: "Toques",
  "Touches in opposition box": "Toques na área rival",
  "Successful dribbles": "Dribles certos",
  Tackles: "Desarmes",
  Interceptions: "Interceptações",
  Recoveries: "Recuperações",
  "Duels won": "Duelos ganhos",
};

const formatMetric = (metric, mode) => {
  const rawValue = mode === "per90" && metric.per90 != null ? metric.per90 : metric.value;
  if (rawValue == null) return "—";
  if (mode === "total" && metric.attempts) {
    const percent = metric.attempts ? Math.round((metric.value / metric.attempts) * 100) : 0;
    return `${Number(metric.value).toLocaleString("pt-BR")} / ${Number(metric.attempts).toLocaleString("pt-BR")} · ${percent}%`;
  }
  return Number(rawValue).toLocaleString("pt-BR", { maximumFractionDigits: 2 });
};

const getPhotoUrl = (photo) => photo?.replace("FORMATO", "220x220") || "";

function PlayerView() {
  const { atletaId } = useParams();
  const initialParams = useMemo(() => new URLSearchParams(window.location.search), []);
  const [statusData, setStatusData] = useState(null);
  const [roundRange, setRoundRange] = useState({
    min: Number(initialParams.get("rodada_min")) || 1,
    max: Number(initialParams.get("rodada_max")) || 1,
  });
  const [venue, setVenue] = useState(initialParams.get("is_mandante") || "geral");
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [statMode, setStatMode] = useState("per90");

  useEffect(() => {
    let active = true;
    fetch("/api/tables/status")
      .then((response) => response.json())
      .then((status) => {
        if (!active) return;
        setStatusData(status);
        if (!initialParams.get("rodada_max")) {
          setRoundRange((current) => ({ ...current, max: status.rodada_atual || 1 }));
        }
      })
      .catch(() => setStatusData({ rodada_atual: Number(initialParams.get("rodada_max")) || 1 }));
    return () => { active = false; };
  }, [initialParams]);

  useEffect(() => {
    if (!statusData) return undefined;
    const params = new URLSearchParams();
    if (roundRange.min !== 1) params.set("rodada_min", roundRange.min);
    if (roundRange.max !== statusData.rodada_atual) params.set("rodada_max", roundRange.max);
    if (venue !== "geral") params.set("is_mandante", venue);
    const query = params.toString();
    window.history.replaceState({}, "", `${window.location.pathname}${query ? `?${query}` : ""}`);

    const controller = new AbortController();
    const timer = window.setTimeout(async () => {
      setLoading(true);
      setError("");
      try {
        const apiParams = new URLSearchParams({
          rodada_min: String(roundRange.min),
          rodada_max: String(roundRange.max),
          is_mandante: venue,
        });
        const response = await fetch(`/api/players/${atletaId}?${apiParams}`, { signal: controller.signal });
        if (!response.ok) throw new Error(response.status === 404 ? "Atleta não encontrado" : "Não foi possível carregar o atleta");
        setData(await response.json());
      } catch (requestError) {
        if (requestError.name !== "AbortError") setError(requestError.message);
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    }, 280);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [atletaId, roundRange, statusData, venue]);

  const backParams = new URLSearchParams();
  if (roundRange.min !== 1) backParams.set("rodada_min", roundRange.min);
  if (statusData && roundRange.max !== statusData.rodada_atual) backParams.set("rodada_max", roundRange.max);
  if (venue !== "geral") backParams.set("is_mandante", venue);
  const backHref = `/atletas${backParams.toString() ? `?${backParams}` : ""}`;

  if (loading && !data) {
    return <div className="player-loading"><span /><p>Montando o dossiê do atleta…</p></div>;
  }
  if (error && !data) {
    return <div className="player-error"><span>!</span><h1>{error}</h1><a href={backHref}>Voltar para atletas</a></div>;
  }

  const cartola = data?.cartola;
  const opta = data?.opta;
  const profile = cartola?.profile || {};
  const bestCartolaMatch = cartola?.matches?.reduce((best, match) => match.points > (best?.points ?? -Infinity) ? match : best, null);
  const cartolaPhoto = getPhotoUrl(profile.photo);
  const fotmobPhoto = opta?.available && opta?.profile?.id
    ? `https://images.fotmob.com/image_resources/playerimages/${opta.profile.id}.png`
    : "";
  const playerPhoto = fotmobPhoto || cartolaPhoto;

  const renderFilterControls = () => (
    <>
      <div className="filter-caption">
        <span>FILTROS</span>
        <b>R{roundRange.min}—R{roundRange.max}</b>
      </div>
      <RoundIntervalSlider
        min={1}
        max={statusData?.rodada_atual || roundRange.max}
        value={roundRange}
        onChange={setRoundRange}
      />
      <MandoToggle value={venue} onChange={setVenue} />
    </>
  );

  return (
    <div className="player-view">
      <a className="player-back" href={backHref}><span>←</span> Voltar para atletas</a>

      <header className="player-hero">
        <div className="player-photo-wrap">
          {playerPhoto ? (
            <img
              src={playerPhoto}
              alt={profile.nickname}
              data-fallback={fotmobPhoto ? cartolaPhoto : ""}
              onError={(event) => {
                const fallback = event.currentTarget.dataset.fallback;
                if (fallback) {
                  event.currentTarget.src = fallback;
                  event.currentTarget.dataset.fallback = "";
                } else {
                  event.currentTarget.style.display = "none";
                }
              }}
            />
          ) : <span>{profile.nickname?.slice(0, 2)}</span>}
          <div className="player-number">{profile.position || "ATL"}</div>
        </div>
        <div className="player-identity">
          <div className="eyebrow"><span className="live-dot" /> Dossiê de performance</div>
          <h1>{profile.nickname}</h1>
          <p>{profile.full_name}</p>
          <div className="player-meta">
            {profile.club_badge && <img src={profile.club_badge} alt="" />}
            <strong>{profile.club_name}</strong>
            <span>{profile.position_name}</span>
            <span className="status-pill">{profile.status}</span>
          </div>
        </div>
      </header>

      <aside className="player-sticky-filters" aria-label="Filtros persistentes do atleta">
        {renderFilterControls()}
      </aside>

      <section className="source-section cartola-source">
        <div className="source-heading">
          <div><span className="source-index">01</span><div><span className="eyebrow">Cartola FC</span><h2>Pontuação fantasy</h2></div></div>
          <p>Scouts e pontos do Cartola no recorte selecionado.</p>
        </div>
        <div className="cartola-overview">
          <article><span>Média</span><strong>{cartola.summary.average_points.toFixed(2)}</strong><small>pontos / jogo</small></article>
          <article><span>Média básica</span><strong>{cartola.summary.average_basic_points.toFixed(2)}</strong><small>sem bônus decisivos</small></article>
          <article><span>Jogos</span><strong>{cartola.summary.matches}</strong><small>{venue === "geral" ? "todos os mandos" : venue}</small></article>
          <article><span>Melhor rodada</span><strong>{bestCartolaMatch ? bestCartolaMatch.points.toFixed(2) : "—"}</strong><small>{bestCartolaMatch ? `R${bestCartolaMatch.round} · ${bestCartolaMatch.opponent_name}` : "sem jogos"}</small></article>
        </div>
        {!!Object.keys(cartola.summary.scouts || {}).length && (
          <div className="scout-ribbon">
            <span>SCOUTS</span>
            {Object.entries(cartola.summary.scouts).map(([key, value]) => <div key={key}><b>{key}</b><strong>{value}</strong></div>)}
          </div>
        )}
        <PlayerPointsChart
          matches={cartola.matches}
          average={cartola.summary.average_points}
        />
      </section>

      <section className="source-section opta-source">
        <div className="source-heading">
          <div><span className="source-index">02</span><div><span className="eyebrow">Opta · via FotMob</span><h2>Performance em campo</h2></div></div>
          <p>Os números Opta têm metodologia própria e não são somados aos scouts Cartola.</p>
        </div>

        {!opta?.available ? (
          <div className="opta-unavailable"><span>OPT</span><div><h3>Opta indisponível para este atleta</h3><p>{opta?.error || "Ainda não foi possível resolver o mapeamento."}</p></div></div>
        ) : (
          <>
            <div className="opta-ticker">
              <div><span>PARTIDAS</span><b>{opta.summary.matches}</b></div>
              <div><span>MINUTOS</span><b>{opta.summary.minutes.toLocaleString("pt-BR")}</b></div>
              <div><span>FINALIZAÇÕES</span><b>{opta.summary.shots}</b></div>
              <div><span>xG</span><b>{Number(opta.summary.xg).toFixed(2)}</b></div>
              <p><i /> Fonte confirmada: {String(opta.data_provider).toUpperCase()}</p>
            </div>

            <PlayerShotMap shots={opta.shots} summary={opta.summary} />

            <section className="player-card performance-card">
              <div className="performance-heading">
                <div><span className="eyebrow">Estatísticas Opta</span><h2>Raio-x da performance</h2><p>{opta.summary.minutes.toLocaleString("pt-BR")} minutos no recorte</p></div>
                <div className="stat-mode" role="group" aria-label="Modo das estatísticas">
                  <button className={statMode === "total" ? "active" : ""} onClick={() => setStatMode("total")}>Total</button>
                  <button className={statMode === "per90" ? "active" : ""} onClick={() => setStatMode("per90")}>Por 90</button>
                </div>
              </div>
              <div className="stat-groups">
                {opta.stat_groups.map((group) => (
                  <article key={group.key} className="stat-group">
                    <h3>{GROUP_LABELS[group.title] || group.title}</h3>
                    {group.metrics.map((metric) => (
                      <div className="stat-line" key={metric.key}>
                        <span>{METRIC_LABELS[metric.title] || metric.title}</span>
                        <strong>{formatMetric(metric, statMode)}</strong>
                      </div>
                    ))}
                  </article>
                ))}
              </div>
            </section>
          </>
        )}
      </section>
    </div>
  );
}

export default PlayerView;

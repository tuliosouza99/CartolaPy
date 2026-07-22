import { useEffect, useMemo, useState } from "react";

const FILTER_LABELS = {
  RegularPlay: "Bola rolando",
  FastBreak: "Contra-ataque",
  FromCorner: "Escanteio",
  SetPiece: "Bola parada",
  Penalty: "Pênalti",
  LeftFoot: "Pé esquerdo",
  RightFoot: "Pé direito",
  Head: "Cabeça",
  InsideBox: "Dentro da área",
  OutsideBox: "Fora da área",
};

const SHOT_RESULT = {
  Goal: "Gol",
  AttemptSaved: "Defendida",
  Miss: "Para fora",
  Post: "Na trave",
  Blocked: "Bloqueada",
};

const getShotResult = (shot) => {
  if (shot.eventType === "Goal") return "Goal";
  if (shot.isBlocked) return "Blocked";
  if (shot.isOnTarget) return "AttemptSaved";
  return shot.eventType || "Miss";
};

const shotColor = (shot) => {
  const result = getShotResult(shot);
  if (result === "Goal") return "#f97316";
  if (result === "AttemptSaved") return "#22c55e";
  if (result === "Blocked") return "#eab308";
  return "#94a3b8";
};

function PlayerShotMap({ shots = [], summary = {} }) {
  const [selectedShot, setSelectedShot] = useState(shots[0] || null);
  const [activeFilters, setActiveFilters] = useState([]);

  useEffect(() => {
    setSelectedShot(shots[0] || null);
    setActiveFilters([]);
  }, [shots]);

  const filterOptions = useMemo(() => {
    const values = new Set();
    shots.forEach((shot) => {
      if (shot.situation) values.add(shot.situation);
      if (shot.shotType) values.add(shot.shotType);
      if (shot.box) values.add(shot.box);
      else values.add(shot.isFromInsideBox ? "InsideBox" : "OutsideBox");
    });
    return [...values].map((value) => ({
      value,
      label: FILTER_LABELS[value] || value,
      count: shots.filter((shot) =>
        [
          shot.situation,
          shot.shotType,
          shot.box || (shot.isFromInsideBox ? "InsideBox" : "OutsideBox"),
        ].includes(value),
      ).length,
    }));
  }, [shots]);

  const visibleShots = useMemo(() => {
    if (!activeFilters.length) return shots;
    return shots.filter((shot) => {
      const values = [
        shot.situation,
        shot.shotType,
        shot.box || (shot.isFromInsideBox ? "InsideBox" : "OutsideBox"),
      ];
      return activeFilters.every((filter) => values.includes(filter));
    });
  }, [shots, activeFilters]);

  const toggleFilter = (filter) => {
    setActiveFilters((current) =>
      current.includes(filter)
        ? current.filter((item) => item !== filter)
        : [...current, filter],
    );
  };

  const mapX = (shot) => 30 + ((Number(shot.y) || 34) / 68) * 620;
  const mapY = (shot) => {
    const attackingX = Math.max(52.5, Math.min(105, Number(shot.x) || 52.5));
    return 28 + ((attackingX - 52.5) / 52.5) * 374;
  };

  if (!shots.length) {
    return (
      <section className="player-card shot-map-empty">
        <span className="eyebrow">Mapa de finalizações</span>
        <h2>Sem finalizações no recorte</h2>
        <p>Altere as rodadas ou o mando para ampliar a amostra Opta.</p>
      </section>
    );
  }

  const onTargetRate = summary.shots
    ? Math.round((summary.shots_on_target / summary.shots) * 100)
    : 0;

  return (
    <section className="player-card shot-map-card">
      <div className="shot-map-heading">
        <div>
          <span className="eyebrow">Mapa de finalizações · Opta</span>
          <h2>Onde a jogada termina</h2>
          <p>{onTargetRate}% das finalizações foram no alvo</p>
        </div>
        <div className="shot-legend" aria-label="Legenda do mapa">
          <span><i className="goal" /> Gol</span>
          <span><i className="target" /> No alvo</span>
          <span><i /> Fora</span>
        </div>
      </div>

      <div className="shot-map-layout">
        <div className="pitch-shell">
          <svg className="shot-pitch" viewBox="0 0 680 430" role="img" aria-label="Mapa de finalizações no campo de ataque">
            <rect x="18" y="18" width="644" height="394" rx="22" className="pitch-fill" />
            <path d="M18 18h644v394H18z" className="pitch-line" />
            <path d="M170 412V300h340v112M285 412v-42h110v42" className="pitch-line" />
            <path d="M266 300a84 84 0 0 1 148 0" className="pitch-line" />
            <circle cx="340" cy="328" r="3.5" className="pitch-mark" />
            <path d="M312 418v8h56v-8" className="goal-frame" />
            {visibleShots.map((shot) => {
              const selected = selectedShot?.id === shot.id;
              return (
                <g key={`${shot.match_id}-${shot.id}`}>
                  <circle
                    cx={mapX(shot)}
                    cy={mapY(shot)}
                    r={selected ? 12 : 8}
                    fill={shotColor(shot)}
                    fillOpacity={selected ? 0.96 : 0.62}
                    stroke={selected ? "#fff" : shotColor(shot)}
                    strokeWidth={selected ? 3 : 1.5}
                    className="shot-point"
                    onClick={() => setSelectedShot(shot)}
                    role="button"
                    tabIndex="0"
                  />
                  {selected && <circle cx={mapX(shot)} cy={mapY(shot)} r="18" className="shot-pulse" />}
                </g>
              );
            })}
          </svg>
          <div className="shot-summary-row">
            <div><strong>{summary.shots || 0}</strong><span>Finalizações</span></div>
            <div><strong>{summary.goals || 0}</strong><span>Gols</span></div>
            <div><strong>{Number(summary.xg || 0).toFixed(2)}</strong><span>xG</span></div>
          </div>
        </div>

        <aside className="shot-inspector">
          {selectedShot ? (
            <>
              <div className="shot-matchline">
                <span>R{selectedShot.round}</span>
                <strong>{selectedShot.is_home ? "casa" : "fora"} · {selectedShot.opponent_name}</strong>
                <span>{selectedShot.score || "—"}</span>
              </div>
              <div className="shot-result">
                <span style={{ background: shotColor(selectedShot) }} />
                {SHOT_RESULT[getShotResult(selectedShot)] || getShotResult(selectedShot)}
                <small>{selectedShot.min}{selectedShot.minAdded ? `+${selectedShot.minAdded}` : ""}&apos;</small>
              </div>
              <dl className="shot-details">
                <div><dt>Execução</dt><dd>{FILTER_LABELS[selectedShot.shotType] || selectedShot.shotType || "—"}</dd></div>
                <div><dt>Situação</dt><dd>{FILTER_LABELS[selectedShot.situation] || selectedShot.situation || "—"}</dd></div>
                <div><dt>xG</dt><dd>{Number(selectedShot.expectedGoals || 0).toFixed(2)}</dd></div>
                <div><dt>xGOT</dt><dd>{selectedShot.expectedGoalsOnTarget != null ? Number(selectedShot.expectedGoalsOnTarget).toFixed(2) : "—"}</dd></div>
              </dl>
            </>
          ) : <p>Selecione uma finalização.</p>}

          <div className="shot-filters">
            <span className="eyebrow">Filtrar mapa</span>
            <div>
              {filterOptions.map((filter) => (
                <button
                  key={filter.value}
                  className={activeFilters.includes(filter.value) ? "active" : ""}
                  onClick={() => toggleFilter(filter.value)}
                >
                  {filter.label} <b>{filter.count}</b>
                </button>
              ))}
            </div>
          </div>
        </aside>
      </div>
    </section>
  );
}

export default PlayerShotMap;

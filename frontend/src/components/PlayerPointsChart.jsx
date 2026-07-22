import { useEffect, useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const SCOUT_LABELS = {
  G: "Gol",
  A: "Assistência",
  FT: "Finalização na trave",
  FD: "Finalização defendida",
  FF: "Finalização para fora",
  FS: "Falta sofrida",
  PS: "Pênalti sofrido",
  V: "Finalização bloqueada",
  I: "Impedimento",
  PP: "Pênalti perdido",
  DS: "Desarme",
  SG: "Jogo sem sofrer gol",
  DE: "Defesa",
  DP: "Defesa de pênalti",
  CV: "Cartão vermelho",
  CA: "Cartão amarelo",
  FC: "Falta cometida",
  GC: "Gol contra",
  GS: "Gol sofrido",
  PC: "Pênalti cometido",
};

function PointsTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const match = payload[0].payload;
  return (
    <div className="points-tooltip">
      <div>
        {match.opponent_badge && <img src={match.opponent_badge} alt="" />}
        <strong>R{match.round} · {match.opponent_name || "Adversário"}</strong>
        <span>{match.is_home ? "CASA" : "FORA"}</span>
      </div>
      <p><b>{Number(match.points).toFixed(2)}</b> pontos <i /> {Number(match.basic_points).toFixed(2)} básicos</p>
      {!!Object.keys(match.scouts || {}).length && (
        <small>{Object.entries(match.scouts).map(([key, value]) => `${value}${key}`).join(" · ")}</small>
      )}
    </div>
  );
}

function PlayerPointsChart({ matches = [], average = 0 }) {
  const chartData = useMemo(
    () => [...matches].sort((a, b) => a.round - b.round),
    [matches],
  );
  const [selectedMatch, setSelectedMatch] = useState(chartData.at(-1) || null);

  useEffect(() => {
    setSelectedMatch(chartData.at(-1) || null);
  }, [chartData]);

  if (!chartData.length) {
    return (
      <section className="cartola-match-card empty">
        <span className="eyebrow">Jogo a jogo</span>
        <h3>Sem pontuações no recorte</h3>
      </section>
    );
  }

  const chartWidth = Math.max(620, chartData.length * 66);

  return (
    <section className="cartola-match-card">
      <div className="cartola-match-heading">
        <div>
          <span className="eyebrow">Jogo a jogo · Cartola FC</span>
          <h3>Pontuação por partida</h3>
          <p>Passe sobre uma barra para ver os scouts; clique para fixar os detalhes abaixo.</p>
        </div>
        <div className="chart-average"><span>MÉDIA DO RECORTE</span><strong>{Number(average).toFixed(2)}</strong></div>
      </div>

      <div className="points-chart-scroll">
        <div style={{ minWidth: chartWidth, width: "100%", height: 255 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} margin={{ top: 22, right: 24, left: 4, bottom: 4 }}>
              <CartesianGrid vertical={false} stroke="var(--border)" strokeDasharray="3 5" />
              <XAxis
                dataKey="round"
                tickFormatter={(round) => `R${round}`}
                tick={{ fill: "var(--text-muted)", fontSize: 11, fontWeight: 700 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tick={{ fill: "var(--text-muted)", fontSize: 10 }}
                axisLine={false}
                tickLine={false}
                width={34}
              />
              <Tooltip content={<PointsTooltip />} cursor={{ fill: "rgba(249, 115, 22, .06)" }} />
              <ReferenceLine y={average} stroke="var(--orange)" strokeDasharray="6 5" strokeOpacity={0.55} />
              <ReferenceLine y={0} stroke="var(--border-strong)" />
              <Bar
                dataKey="points"
                radius={[5, 5, 0, 0]}
                maxBarSize={34}
                isAnimationActive={false}
                cursor="pointer"
              >
                {chartData.map((match) => (
                  <Cell
                    key={`${match.round}-${match.match_id}`}
                    onClick={() => setSelectedMatch(match)}
                    cursor="pointer"
                    fill={match.points < 0 ? "#ef4444" : "var(--orange)"}
                    fillOpacity={selectedMatch?.round === match.round ? 1 : 0.58}
                    stroke={selectedMatch?.round === match.round ? "var(--text-primary)" : "transparent"}
                    strokeWidth={1}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {selectedMatch && (
        <div className="selected-match-rail">
          <div className="selected-opponent">
            {selectedMatch.opponent_badge && <img src={selectedMatch.opponent_badge} alt="" />}
            <div><span>RODADA {selectedMatch.round} · {selectedMatch.is_home ? "MANDANTE" : "VISITANTE"}</span><strong>{selectedMatch.opponent_name || "Adversário"}</strong></div>
          </div>
          <div className="selected-score"><span>PONTOS</span><strong>{Number(selectedMatch.points).toFixed(2)}</strong></div>
          <div className="selected-score basic"><span>BÁSICA</span><strong>{Number(selectedMatch.basic_points).toFixed(2)}</strong></div>
          <div className="selected-scouts">
            <span>SCOUTS DA PARTIDA</span>
            <div>
              {Object.entries(selectedMatch.scouts || {}).length ? Object.entries(selectedMatch.scouts).map(([key, value]) => (
                <span key={key} title={SCOUT_LABELS[key] || key}><b>{key}</b>{value}</span>
              )) : <em>Nenhum scout registrado</em>}
            </div>
          </div>
          {!!selectedMatch.match_id && (
            <a href={`/confrontos?rodada=${selectedMatch.round}&partida_id=${selectedMatch.match_id}`} aria-label={`Abrir confronto da rodada ${selectedMatch.round}`}>↗</a>
          )}
        </div>
      )}
    </section>
  );
}

export default PlayerPointsChart;

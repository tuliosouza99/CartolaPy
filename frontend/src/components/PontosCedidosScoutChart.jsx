import { useEffect, useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { SCOUT_BY_CODE } from "./ScoutSelect";
import "./PontosAnalysisChart.css";

const signed = (value) => `${value > 0 ? "+" : ""}${Number(value || 0).toFixed(2)}`;

function MatchTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const match = payload[0].payload;
  return (
    <div className="analysis-tooltip">
      <div><img src={match.opponent_escudo} alt="" /><strong>{match.opponent_nome}</strong><span>R{match.rodada_id}</span></div>
      <p><b>{signed(match.pontuacao)}</b> pts <i /> básica {signed(match.pontuacao_basica)}</p>
      <small>{match.display_is_mandante ? "Mandante" : "Visitante"} · clique para fixar</small>
    </div>
  );
}

function PontosCedidosScoutChart({
  scoutContributions,
  mediaCedida,
  positionScouts,
  totalJogos,
  matches,
  matchesLoading = false,
  matchLabel = "Média por confronto",
  invertVenue = false,
}) {
  const contributions = useMemo(() => (
    (positionScouts || [])
      .filter((code) => scoutContributions?.[code]?.points_contribution)
      .map((code) => ({
        code,
        name: SCOUT_BY_CODE[code]?.name || code,
        occurrences: Number(scoutContributions[code].raw_sum || 0),
        points: Number(scoutContributions[code].points_contribution || 0),
        percentage: Number(scoutContributions[code].percentage || 0),
      }))
      .sort((a, b) => Math.abs(b.points) - Math.abs(a.points))
  ), [positionScouts, scoutContributions]);

  const matchData = useMemo(
    () => [...(matches || [])]
      .map((match) => ({
        ...match,
        display_is_mandante: invertVenue ? !match.is_mandante : match.is_mandante,
      }))
      .sort((a, b) => a.rodada_id - b.rodada_id),
    [invertVenue, matches],
  );
  const [selectedMatch, setSelectedMatch] = useState(null);

  useEffect(() => {
    setSelectedMatch(matchData.at(-1) || null);
  }, [matchData]);

  const maxContribution = Math.max(...contributions.map((item) => Math.abs(item.points)), 0.01);
  const positivePoints = contributions.reduce((sum, item) => sum + Math.max(item.points, 0), 0);
  const negativePoints = contributions.reduce((sum, item) => sum + Math.min(item.points, 0), 0);

  return (
    <section className="points-analysis-card">
      <header className="points-analysis-heading">
        <div><span>DECOMPOSIÇÃO CARTOLA</span><h3>Raio-x da média</h3><p>Ocorrências e pontos médios no recorte selecionado.</p></div>
        <div className="analysis-kpis">
          <div><span>MÉDIA</span><strong>{signed(mediaCedida)}</strong></div>
          <div><span>JOGOS</span><strong>{totalJogos || 0}</strong></div>
          <div className="positive"><span>POSITIVOS</span><strong>{signed(positivePoints)}</strong></div>
          <div className="negative"><span>NEGATIVOS</span><strong>{signed(negativePoints)}</strong></div>
        </div>
      </header>

      <div className="points-analysis-grid">
        <div className="contribution-panel">
          <div className="analysis-panel-title"><span>Contribuição por scout</span><small>pts por atleta / jogo</small></div>
          {contributions.length ? (
            <div className="contribution-list">
              {contributions.map((item) => {
                const width = `${(Math.abs(item.points) / maxContribution) * 50}%`;
                return (
                  <div className="contribution-row" key={item.code} title={`${item.name}: ${item.occurrences.toFixed(2)} ocorrências por atleta/jogo`}>
                    <div className="contribution-name"><b>{item.code}</b><span>{item.name}</span></div>
                    <div className="contribution-track">
                      <i />
                      <span className={item.points >= 0 ? "bar positive" : "bar negative"} style={{ width }} />
                    </div>
                    <strong className={item.points >= 0 ? "positive" : "negative"}>{signed(item.points)}</strong>
                    <small>{Math.abs(item.percentage).toFixed(1)}%</small>
                  </div>
                );
              })}
            </div>
          ) : <div className="analysis-empty">Nenhum scout registrado</div>}
        </div>

        <div className="matches-panel">
          <div className="analysis-panel-title"><span>{matchLabel}</span><small>barra: total · linha: básica</small></div>
          {matchesLoading ? <div className="analysis-empty">Carregando confrontos…</div> : !matchData.length ? (
            <div className="analysis-empty">Nenhum confronto encontrado</div>
          ) : (
            <>
              <div className="analysis-match-chart">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={matchData} margin={{ top: 16, right: 12, left: -20, bottom: 0 }}>
                    <CartesianGrid stroke="rgba(255,255,255,.07)" vertical={false} />
                    <XAxis dataKey="rodada_id" tickFormatter={(value) => `R${value}`} tick={{ fill: "#8b9098", fontSize: 10 }} axisLine={false} tickLine={false} />
                    <YAxis tick={{ fill: "#8b9098", fontSize: 10 }} axisLine={false} tickLine={false} width={38} />
                    <ReferenceLine y={0} stroke="#656a72" />
                    <Tooltip content={<MatchTooltip />} cursor={{ fill: "rgba(249,115,22,.06)" }} />
                    <Bar dataKey="pontuacao" radius={[5, 5, 0, 0]} maxBarSize={28} isAnimationActive={false}>
                      {matchData.map((match) => (
                        <Cell
                          key={`${match.partida_id}-${match.rodada_id}`}
                          onClick={() => setSelectedMatch(match)}
                          cursor="pointer"
                          fill={match.pontuacao >= 0 ? "#f97316" : "#ef4444"}
                          opacity={selectedMatch?.partida_id === match.partida_id ? 1 : .62}
                        />
                      ))}
                    </Bar>
                    <Line
                      type="monotone"
                      dataKey="pontuacao_basica"
                      stroke="#f8fafc"
                      strokeWidth={2}
                      pointerEvents="none"
                      isAnimationActive={false}
                      dot={{ r: 2, fill: "#f8fafc", pointerEvents: "none" }}
                      activeDot={{ r: 4, pointerEvents: "none" }}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              {selectedMatch && (
                <div className="analysis-match-rail">
                  <img src={selectedMatch.opponent_escudo} alt="" />
                  <div><span>R{selectedMatch.rodada_id} · {selectedMatch.display_is_mandante ? "MANDANTE" : "VISITANTE"}</span><strong>{selectedMatch.opponent_nome}</strong></div>
                  <div><span>TOTAL</span><strong>{signed(selectedMatch.pontuacao)}</strong></div>
                  <div><span>BÁSICA</span><strong>{signed(selectedMatch.pontuacao_basica)}</strong></div>
                  <a href={`/confrontos?rodada=${selectedMatch.rodada_id}&partida_id=${selectedMatch.partida_id}`} aria-label="Abrir confronto">↗</a>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </section>
  );
}

export default PontosCedidosScoutChart;

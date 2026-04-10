import { useMemo } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
  Cell,
} from "recharts";

function PontosCedidosScoutChart({
  scoutContributions,
  mediaCedida,
  positionScouts,
  totalJogos,
}) {
  const chartData = useMemo(() => {
    if (!scoutContributions || Object.keys(scoutContributions).length === 0) {
      return [];
    }

    const filteredScouts = (positionScouts || []).filter(
      (scout) =>
        scoutContributions[scout] && scoutContributions[scout].percentage !== 0,
    );

    const jogos = totalJogos || 1;

    return filteredScouts
      .map((scout) => {
        const contrib = scoutContributions[scout];
        return {
          scout,
          percentage: contrib?.percentage || 0,
          perGame: contrib?.raw_sum || 0,
          pointsPerGame: contrib?.points_contribution || 0,
        };
      })
      .sort((a, b) => b.percentage - a.percentage);
  }, [scoutContributions, positionScouts, totalJogos]);

  if (chartData.length === 0) {
    return (
      <div
        style={{
          padding: "1rem",
          color: "var(--text-muted)",
          fontSize: "0.875rem",
          textAlign: "center",
        }}
      >
        Nenhum scout registrado
      </div>
    );
  }

  const maxAbsPercentage = Math.max(
    ...chartData.map((d) => Math.abs(d.percentage)),
    100,
  );

  const chartHeight = Math.max(chartData.length * 28, 120);

  return (
    <div>
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
        Contribuição por Scout
      </div>
      <div style={{ height: `${chartHeight}px` }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            layout="vertical"
            margin={{ top: 5, right: 60, left: 0, bottom: 5 }}
          >
            <CartesianGrid
              strokeDasharray="3 3"
              stroke="var(--border)"
              horizontal={false}
            />
            <XAxis
              type="number"
              domain={[-maxAbsPercentage, maxAbsPercentage]}
              tick={{ fontSize: 10, fill: "var(--text-secondary)" }}
              tickFormatter={(val) => `${Math.abs(val)}%`}
            />
            <YAxis
              type="category"
              dataKey="scout"
              tick={{ fontSize: 11, fill: "var(--text-secondary)" }}
              width={50}
              tickLine={false}
              interval={0}
            />
            <Tooltip content={<CustomTooltip mediaCedida={mediaCedida} />} />
            <ReferenceLine x={0} stroke="var(--border)" />
            <Bar dataKey="percentage" radius={[0, 4, 4, 0]} barSize={16}>
              {chartData.map((entry) => (
                <Cell
                  key={entry.scout}
                  fill={
                    entry.percentage >= 0
                      ? "var(--orange)"
                      : "var(--text-muted)"
                  }
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function CustomTooltip({ active, payload, mediaCedida }) {
  if (!active || !payload || !payload.length) return null;

  const data = payload[0]?.payload;

  return (
    <div
      style={{
        background: "var(--bg-card)",
        border: "1px solid var(--border)",
        borderRadius: "var(--radius-sm)",
        padding: "0.75rem",
        fontSize: "0.75rem",
        boxShadow: "var(--shadow-md)",
      }}
    >
      <div style={{ marginBottom: "0.5rem" }}>
        <span style={{ color: "var(--text-secondary)" }}>Scout: </span>
        <span style={{ fontWeight: 600, color: "var(--text-primary)" }}>
          {data?.scout}
        </span>
      </div>
      <div style={{ marginBottom: "0.25rem" }}>
        <span style={{ color: "var(--text-secondary)" }}>Pontos/Jogo: </span>
        <span style={{ fontWeight: 600, color: "var(--orange)" }}>
          {Number(data?.pointsPerGame || 0).toFixed(2)}
        </span>
      </div>
      <div style={{ marginBottom: "0.25rem" }}>
        <span style={{ color: "var(--text-secondary)" }}>
          Ocorrências/Jogo:{" "}
        </span>
        <span style={{ fontWeight: 600, color: "var(--text-primary)" }}>
          {Number(data?.perGame || 0).toFixed(2)}
        </span>
      </div>
      <div>
        <span style={{ color: "var(--text-secondary)" }}>Contribuição: </span>
        <span
          style={{
            fontWeight: 600,
            color:
              data?.percentage >= 0 ? "var(--orange)" : "var(--text-muted)",
          }}
        >
          {data?.percentage >= 0 ? "+" : ""}
          {Number(data?.percentage || 0).toFixed(1)}%
        </span>
      </div>
    </div>
  );
}

export default PontosCedidosScoutChart;

import { useState, useEffect, useCallback, useRef } from "react";
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

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload || !payload.length) return null;
  const data = payload[0].payload;
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
      <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", marginBottom: "0.5rem" }}>
        <img
          src={data.opponent_escudo}
          alt={data.opponent_nome}
          style={{ width: "20px", height: "20px", objectFit: "contain" }}
          onError={(e) => { e.target.style.display = "none"; }}
        />
        <span style={{ fontWeight: 600, color: "var(--text-primary)" }}>
          {data.opponent_nome || "Adversário desconhecido"}
        </span>
        <span
          style={{
            padding: "0.125rem 0.375rem",
            background: data.is_mandante ? "var(--orange)" : "var(--bg-secondary)",
            color: data.is_mandante ? "white" : "var(--text-secondary)",
            borderRadius: "var(--radius-sm)",
            fontWeight: 600,
            fontSize: "0.65rem",
          }}
        >
          {data.is_mandante ? "M" : "V"}
        </span>
      </div>
      <div style={{ display: "flex", gap: "1rem", marginBottom: "0.25rem" }}>
        <div>
          <span style={{ color: "var(--text-secondary)" }}>Pontuação: </span>
          <span style={{ fontWeight: 600, color: "var(--orange)" }}>
            {Number(data.pontuacao).toFixed(1)}
          </span>
        </div>
        <div>
          <span style={{ color: "var(--text-secondary)" }}>Básica: </span>
          <span style={{ fontWeight: 600, color: "var(--text-primary)" }}>
            {Number(data.pontuacao_basica).toFixed(1)}
          </span>
        </div>
      </div>
      {data.scouts && Object.keys(data.scouts).length > 0 && (
        <div style={{ marginTop: "0.5rem", paddingTop: "0.5rem", borderTop: "1px solid var(--border)" }}>
          <span style={{ color: "var(--text-secondary)" }}>Scouts: </span>
          <span style={{ fontFamily: "var(--font-mono)", color: "var(--text-primary)" }}>
            {Object.entries(data.scouts)
              .filter(([_, v]) => v !== 0)
              .map(([k, v]) => `${v}${k}`)
              .join(" ")}
          </span>
        </div>
      )}
    </div>
  );
};

function AtletaCharts({ atleta_id, rodadaRange, isMandante, media, media_basica }) {
  const [historicoData, setHistoricoData] = useState(null);
  const [loading, setLoading] = useState(true);
  const loadingRef = useRef(false);

  const fetchHistorico = useCallback(async () => {
    const params = new URLSearchParams({
      rodada_min: rodadaRange.min,
      rodada_max: rodadaRange.max,
    });
    if (isMandante !== "geral") {
      params.set("is_mandante", isMandante);
    }
    try {
      const res = await fetch(
        `/api/tables/atletas/${atleta_id}/historico?${params}`
      );
      if (res.ok) {
        const data = await res.json();
        const sorted = [...(data.historico || [])].sort(
          (a, b) => a.rodada_id - b.rodada_id
        );
        setHistoricoData(sorted);
      }
    } catch (err) {
      console.error("Failed to fetch historico:", err);
    }
  }, [atleta_id, rodadaRange, isMandante]);

  useEffect(() => {
    if (loadingRef.current) return;
    loadingRef.current = true;
    setLoading(true);

    fetchHistorico().finally(() => {
      setLoading(false);
      loadingRef.current = false;
    });
  }, [fetchHistorico]);

  const handleBarClick = (data) => {
    if (data && data.partida_id) {
      window.open(
        `/confrontos?rodada=${data.rodada_id}&partida_id=${data.partida_id}`,
        "_blank",
        "noopener,noreferrer"
      );
    }
  };

  if (loading) {
    return (
      <div
        style={{
          display: "flex",
          justifyContent: "center",
          padding: "2rem",
          color: "var(--text-muted)",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <div
            style={{
              width: "16px",
              height: "16px",
              border: "2px solid var(--border)",
              borderTopColor: "var(--orange)",
              borderRadius: "50%",
              animation: "spin 0.8s linear infinite",
            }}
          />
          Carregando gráfico...
        </div>
      </div>
    );
  }

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
        Histórico de Pontuação
      </div>
      <div style={{ height: "200px" }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={historicoData || []}
            margin={{ top: 5, right: 30, left: 0, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
            <XAxis
              dataKey="rodada_id"
              tick={{ fontSize: 11, fill: "var(--text-secondary)" }}
              tickFormatter={(val) => `R${val}`}
            />
            <YAxis
              tick={{ fontSize: 11, fill: "var(--text-secondary)" }}
              width={30}
            />
            <Tooltip content={<CustomTooltip />} />
            <ReferenceLine y={media} stroke="var(--orange)" strokeDasharray="5 5" />
            <ReferenceLine y={media_basica} stroke="var(--text-secondary)" strokeDasharray="5 5" />
            <Bar
              dataKey="pontuacao"
              name="Pontuação"
              fill="var(--orange)"
              radius={[4, 4, 0, 0]}
              cursor="pointer"
              onClick={handleBarClick}
            >
              {(historicoData || []).map((entry, index) => (
                <Cell key={`cell-${index}`} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      <style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
}

export default AtletaCharts;

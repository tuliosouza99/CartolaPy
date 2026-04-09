const SCOUTS = [
  { code: "G", name: "Gol", value: 8 },
  { code: "A", name: "Assistência", value: 5 },
  { code: "FT", name: "Finalização na trave", value: 3 },
  { code: "FD", name: "Finalização defendida", value: 1.2 },
  { code: "FF", name: "Finalização pra fora", value: 0.8 },
  { code: "FS", name: "Falta sofrida", value: 0.5 },
  { code: "PS", name: "Pênalti sofrido", value: 1 },
  { code: "V", name: "Vitória", value: 1 },
  { code: "I", name: "Impedimento", value: -0.1 },
  { code: "PP", name: "Pênalti perdido", value: -4 },
  { code: "DS", name: "Desarme", value: 1.2 },
  { code: "SG", name: "Jogo sem sofrer gol", value: 5 },
  { code: "DE", name: "Defesa", value: 1 },
  { code: "DP", name: "Defesa de pênalti", value: 7 },
  { code: "CV", name: "Cartão vermelho", value: -3 },
  { code: "CA", name: "Cartão amarelo", value: -1 },
  { code: "FC", name: "Falta cometida", value: -0.3 },
  { code: "GC", name: "Gol contra", value: -3 },
  { code: "GS", name: "Gol sofrido", value: -1 },
  { code: "PC", name: "Pênalti cometido", value: -1 },
];

const SCOUT_BY_CODE = SCOUTS.reduce((acc, s) => ({ ...acc, [s.code]: s }), {});

function ScoutSelect({ value, onChange, scouts }) {
  const scoutList = scouts
    ? scouts.map((code) => SCOUT_BY_CODE[code]).filter(Boolean)
    : SCOUTS;

  return (
    <select
      value={value || ""}
      onChange={(e) => onChange(e.target.value || null)}
      style={{
        padding: "0.375rem 0.75rem",
        background: "var(--bg-card)",
        border: "1px solid var(--border)",
        borderRadius: "var(--radius)",
        color: "var(--text-primary)",
        fontSize: "0.875rem",
        cursor: "pointer",
        minWidth: "120px",
      }}
    >
      <option value="">Ordenar por Scout</option>
      {scoutList.map((scout) => (
        <option key={scout.code} value={scout.code}>
          {scout.code} - {scout.name}
        </option>
      ))}
    </select>
  );
}

export { SCOUTS, SCOUT_BY_CODE };
export default ScoutSelect;

function MandoToggle({ value, onChange }) {
  const options = [
    { value: 'geral', label: 'Geral' },
    { value: 'mandante', label: 'Mandante' },
    { value: 'visitante', label: 'Visitante' },
  ]

  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: '0.25rem',
      background: 'var(--bg-secondary)',
      padding: '0.25rem',
      borderRadius: 'var(--radius-sm)',
      fontFamily: 'var(--font-display)',
      fontSize: '0.875rem',
    }}>
      {options.map((option) => (
        <button
          key={option.value}
          onClick={() => onChange?.(option.value)}
          style={{
            padding: '0.375rem 0.75rem',
            border: 'none',
            borderRadius: 'var(--radius-sm)',
            background: value === option.value ? 'var(--orange)' : 'transparent',
            color: value === option.value ? 'white' : 'var(--text-secondary)',
            fontWeight: value === option.value ? 600 : 500,
            cursor: 'pointer',
            transition: 'all var(--transition)',
          }}
        >
          {option.label}
        </button>
      ))}
    </div>
  )
}

export default MandoToggle

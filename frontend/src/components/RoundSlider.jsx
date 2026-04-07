import { useState, useEffect } from 'react'

function RoundSlider({ min = 1, max, value, onChange }) {
  const [localValue, setLocalValue] = useState(value || max)

  useEffect(() => {
    if (max !== undefined) {
      setLocalValue(prev => Math.min(prev, max))
    }
  }, [max])

  const handleChange = (e) => {
    const newValue = parseInt(e.target.value, 10)
    setLocalValue(newValue)
    onChange?.(newValue)
  }

  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: '0.75rem',
      fontFamily: 'var(--font-display)',
      fontSize: '0.875rem',
    }}>
      <span style={{ color: 'var(--text-secondary)', fontWeight: 500 }}>
        Até Rodada:
      </span>
      <input
        type="range"
        min={min}
        max={max}
        value={localValue}
        onChange={handleChange}
        style={{
          width: '120px',
          accentColor: 'var(--orange)',
        }}
      />
      <span style={{
        color: 'var(--text-primary)',
        fontWeight: 600,
        minWidth: '24px',
        textAlign: 'center',
      }}>
        {localValue}
      </span>
    </div>
  )
}

export default RoundSlider

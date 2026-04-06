import { useState, useEffect } from 'react'

function RoundIntervalSlider({ min = 1, max, value, onChange }) {
  const [localMin, setLocalMin] = useState(value?.min ?? min)
  const [localMax, setLocalMax] = useState(value?.max ?? max)

  useEffect(() => {
    if (max !== undefined) {
      setLocalMax(max)
    }
  }, [max])

  useEffect(() => {
    if (value?.min !== undefined) setLocalMin(value.min)
    if (value?.max !== undefined) setLocalMax(value.max)
  }, [value])

  const getPercent = (val) => ((val - min) / (max - min)) * 100

  const handleMinChange = (e) => {
    const newMin = parseInt(e.target.value, 10)
    if (newMin <= localMax - 1) {
      setLocalMin(newMin)
      onChange?.({ min: newMin, max: localMax })
    }
  }

  const handleMaxChange = (e) => {
    const newMax = parseInt(e.target.value, 10)
    if (newMax >= localMin + 1) {
      setLocalMax(newMax)
      onChange?.({ min: localMin, max: newMax })
    }
  }

  const minPercent = getPercent(localMin)
  const maxPercent = getPercent(localMax)

  return (
    <>
      <style>{`
        .dual-slider {
          position: relative;
          width: 200px;
          height: 24px;
        }
        .dual-slider-track {
          position: absolute;
          top: 50%;
          transform: translateY(-50%);
          width: 100%;
          height: 4px;
          background: var(--border);
          border-radius: 2px;
        }
        .dual-slider-range {
          position: absolute;
          top: 50%;
          transform: translateY(-50%);
          height: 4px;
          background: var(--orange);
          border-radius: 2px;
        }
        .dual-slider input[type="range"] {
          position: absolute;
          top: 50%;
          transform: translateY(-50%);
          width: 100%;
          height: 4px;
          background: transparent;
          -webkit-appearance: none;
          appearance: none;
          pointer-events: none;
          margin: 0;
        }
        .dual-slider input[type="range"]::-webkit-slider-thumb {
          -webkit-appearance: none;
          appearance: none;
          width: 16px;
          height: 16px;
          background: var(--orange);
          border-radius: 50%;
          cursor: pointer;
          pointer-events: auto;
          border: 2px solid white;
          box-shadow: 0 1px 3px rgba(0,0,0,0.3);
        }
        .dual-slider input[type="range"]::-moz-range-thumb {
          width: 16px;
          height: 16px;
          background: var(--orange);
          border-radius: 50%;
          cursor: pointer;
          pointer-events: auto;
          border: 2px solid white;
          box-shadow: 0 1px 3px rgba(0,0,0,0.3);
        }
        .dual-slider-labels {
          display: flex;
          justify-content: space-between;
          width: 100%;
          padding: 0 8px;
          margin-top: 16px;
        }
        .dual-slider-labels span {
          color: var(--text-primary);
          font-weight: 600;
          font-size: 0.75rem;
        }
      `}</style>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: '0.75rem',
        fontFamily: 'var(--font-display)',
        fontSize: '0.875rem',
      }}>
        <span style={{ color: 'var(--text-secondary)', fontWeight: 500 }}>
          Rodada:
        </span>
        <div className="dual-slider">
          <div className="dual-slider-track" />
          <div
            className="dual-slider-range"
            style={{ left: `${minPercent}%`, width: `${maxPercent - minPercent}%` }}
          />
          <input
            type="range"
            min={min}
            max={max}
            value={localMin}
            onChange={handleMinChange}
            style={{ zIndex: localMin > max - 2 ? 5 : 4 }}
          />
          <input
            type="range"
            min={min}
            max={max}
            value={localMax}
            onChange={handleMaxChange}
            style={{ zIndex: 5 }}
          />
          <div className="dual-slider-labels">
            <span>{localMin}</span>
            <span>{localMax}</span>
          </div>
        </div>
      </div>
    </>
  )
}

export default RoundIntervalSlider
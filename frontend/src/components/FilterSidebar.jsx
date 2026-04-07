import { useState, useMemo } from 'react'

function debounce(fn, delay) {
  let timeoutId
  return (...args) => {
    clearTimeout(timeoutId)
    timeoutId = setTimeout(() => fn(...args), delay)
  }
}

function FilterSidebar({ filters, onFiltersChange, disabled }) {
  const [searchInput, setSearchInput] = useState(filters.search || '')

  const debouncedSearch = useMemo(
    () => debounce((value) => onFiltersChange({ ...filters, search: value }), 300),
    [filters, onFiltersChange]
  )

  const handleSearchChange = (e) => {
    const value = e.target.value
    setSearchInput(value)
    debouncedSearch(value)
  }

  const handleClubeToggle = (clubeId) => {
    const current = filters.clube_ids || []
    const updated = current.includes(clubeId)
      ? current.filter(id => id !== clubeId)
      : [...current, clubeId]
    onFiltersChange({ ...filters, clube_ids: updated })
  }

  const handlePosicaoToggle = (posicaoId) => {
    const current = filters.posicao_ids || []
    const updated = current.includes(posicaoId)
      ? current.filter(id => id !== posicaoId)
      : [...current, posicaoId]
    onFiltersChange({ ...filters, posicao_ids: updated })
  }

  const handleStatusToggle = (statusId) => {
    const current = filters.status_ids || []
    const updated = current.includes(statusId)
      ? current.filter(id => id !== statusId)
      : [...current, statusId]
    onFiltersChange({ ...filters, status_ids: updated })
  }

  const handlePrecoChange = (range) => {
    onFiltersChange({ ...filters, preco_min: range.min, preco_max: range.max })
  }

  const clearAll = () => {
    setSearchInput('')
    onFiltersChange({
      search: '',
      clube_ids: [],
      posicao_ids: [],
      status_ids: [],
      preco_min: 0,
      preco_max: 30,
    })
  }

  const hasActiveFilters = 
    (filters.search && filters.search.length > 0) ||
    (filters.clube_ids && filters.clube_ids.length > 0) ||
    (filters.posicao_ids && filters.posicao_ids.length > 0) ||
    (filters.status_ids && filters.status_ids.length > 0) ||
    filters.preco_min !== 0 ||
    filters.preco_max !== 30

  const { clubes, posicoes, status, precoRange } = filters.options || {}

  return (
    <>
      <style>{`
        .filter-section {
          margin-bottom: 1.5rem;
        }
        .filter-section-title {
          font-family: var(--font-display);
          font-size: 0.75rem;
          font-weight: 600;
          color: var(--text-muted);
          text-transform: uppercase;
          letter-spacing: 0.05em;
          margin-bottom: 0.75rem;
        }
        .filter-search {
          width: 100%;
          padding: 0.5rem 0.75rem;
          background: var(--bg-secondary);
          border: 1px solid var(--border);
          border-radius: var(--radius-sm);
          color: var(--text-primary);
          font-family: var(--font-display);
          font-size: 0.875rem;
        }
        .filter-search:focus {
          outline: none;
          border-color: var(--orange);
        }
        .filter-checkbox-group {
          display: flex;
          flex-direction: column;
          gap: 0.5rem;
          max-height: 200px;
          overflow-y: auto;
        }
        .filter-checkbox-item {
          display: flex;
          align-items: center;
          gap: 0.5rem;
          cursor: pointer;
        }
        .filter-checkbox-item input {
          accent-color: var(--orange);
          cursor: pointer;
        }
        .filter-checkbox-item label {
          font-family: var(--font-display);
          font-size: 0.8rem;
          color: var(--text-secondary);
          cursor: pointer;
        }
        .filter-checkbox-item:hover label {
          color: var(--text-primary);
        }
        .filter-slider-container {
          padding: 0.5rem 0;
        }
        .filter-slider-labels {
          display: flex;
          justify-content: space-between;
          margin-top: 0.5rem;
          font-family: var(--font-display);
          font-size: 0.75rem;
          color: var(--text-muted);
        }
        .filter-clear-btn {
          width: 100%;
          padding: 0.5rem;
          background: transparent;
          border: 1px solid var(--border);
          border-radius: var(--radius-sm);
          color: var(--text-secondary);
          font-family: var(--font-display);
          font-size: 0.75rem;
          cursor: pointer;
          margin-top: 1rem;
          transition: all var(--transition);
        }
        .filter-clear-btn:hover {
          border-color: var(--orange);
          color: var(--orange);
        }
        .dual-slider {
          position: relative;
          width: 100%;
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
      `}</style>

      <div style={{
        width: '260px',
        background: 'var(--bg-card)',
        borderRadius: 'var(--radius-lg)',
        border: '1px solid var(--border)',
        padding: '1rem',
        height: 'fit-content',
        position: 'sticky',
        top: '1rem',
      }}>
        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '1rem',
        }}>
          <span style={{
            fontFamily: 'var(--font-display)',
            fontSize: '0.875rem',
            fontWeight: 600,
            color: 'var(--text-primary)',
          }}>
            Filtros
          </span>
          {hasActiveFilters && (
            <button 
              onClick={clearAll}
              className="filter-clear-btn"
              style={{
                width: 'auto',
                padding: '0.25rem 0.5rem',
                marginTop: 0,
              }}
            >
              Limpar
            </button>
          )}
        </div>

        <div className="filter-section">
          <div className="filter-section-title">Buscar</div>
          <input
            type="text"
            className="filter-search"
            placeholder="Nome do jogador..."
            value={searchInput}
            onChange={handleSearchChange}
            disabled={disabled}
          />
        </div>

        {clubes && clubes.length > 0 && (
          <div className="filter-section">
            <div className="filter-section-title">Clube</div>
            <div className="filter-checkbox-group">
              {clubes.map(clube => (
                <div key={clube.id} className="filter-checkbox-item">
                  <input
                    type="checkbox"
                    id={`clube-${clube.id}`}
                    checked={filters.clube_ids?.includes(clube.id) || false}
                    onChange={() => handleClubeToggle(clube.id)}
                    disabled={disabled}
                  />
                  <label htmlFor={`clube-${clube.id}`}>
                    {clube.nome_fantasia}
                  </label>
                </div>
              ))}
            </div>
          </div>
        )}

        {posicoes && posicoes.length > 0 && (
          <div className="filter-section">
            <div className="filter-section-title">Posição</div>
            <div className="filter-checkbox-group">
              {posicoes.map(pos => (
                <div key={pos.id} className="filter-checkbox-item">
                  <input
                    type="checkbox"
                    id={`posicao-${pos.id}`}
                    checked={filters.posicao_ids?.includes(pos.id) || false}
                    onChange={() => handlePosicaoToggle(pos.id)}
                    disabled={disabled}
                  />
                  <label htmlFor={`posicao-${pos.id}`}>
                    {pos.nome}
                  </label>
                </div>
              ))}
            </div>
          </div>
        )}

        {status && status.length > 0 && (
          <div className="filter-section">
            <div className="filter-section-title">Status</div>
            <div className="filter-checkbox-group">
              {status.map(s => (
                <div key={s.id} className="filter-checkbox-item">
                  <input
                    type="checkbox"
                    id={`status-${s.id}`}
                    checked={filters.status_ids?.includes(s.id) || false}
                    onChange={() => handleStatusToggle(s.id)}
                    disabled={disabled}
                  />
                  <label htmlFor={`status-${s.id}`}>
                    {s.nome}
                  </label>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="filter-section">
          <div className="filter-section-title">Preço (C$)</div>
          <div className="filter-slider-container">
            <div className="dual-slider">
              <div className="dual-slider-track" />
              <div
                className="dual-slider-range"
                style={{
                  left: `${(filters.preco_min / 30) * 100}%`,
                  width: `${((filters.preco_max - filters.preco_min) / 30) * 100}%`,
                }}
              />
              <input
                type="range"
                min={0}
                max={30}
                value={filters.preco_min || 0}
                onChange={(e) => {
                  const newMin = parseInt(e.target.value, 10)
                  if (newMin <= filters.preco_max - 1) {
                    handlePrecoChange({ min: newMin, max: filters.preco_max })
                  }
                }}
                style={{ zIndex: filters.preco_min > 28 ? 5 : 4 }}
                disabled={disabled}
              />
              <input
                type="range"
                min={0}
                max={30}
                value={filters.preco_max || 30}
                onChange={(e) => {
                  const newMax = parseInt(e.target.value, 10)
                  if (newMax >= filters.preco_min + 1) {
                    handlePrecoChange({ min: filters.preco_min, max: newMax })
                  }
                }}
                style={{ zIndex: 5 }}
                disabled={disabled}
              />
            </div>
            <div className="filter-slider-labels">
              <span>C$ {filters.preco_min || 0}</span>
              <span>C$ {filters.preco_max || 30}</span>
            </div>
          </div>
        </div>
      </div>
    </>
  )
}

export default FilterSidebar
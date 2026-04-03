import { useState, useEffect, useCallback } from 'react'

function TableView({ title, endpoint, columns, lastUpdated, renderCell, action }) {
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)
  const [pageSize] = useState(20)
  const [sortBy, setSortBy] = useState(null)
  const [sortDirection, setSortDirection] = useState('asc')

  const fetchData = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        page_size: pageSize.toString(),
      })
      if (sortBy) {
        params.append('sort_by', sortBy)
        params.append('sort_direction', sortDirection)
      }
      const res = await fetch(`/api/tables/${endpoint}?${params}`)
      if (!res.ok) throw new Error('Failed to fetch data')
      const json = await res.json()
      setData(json.data)
      setTotal(json.total)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }, [endpoint, page, pageSize, sortBy, sortDirection])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  const handleSort = (column) => {
    if (sortBy === column) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(column)
      setSortDirection('asc')
    }
    setPage(1)
  }

  const totalPages = Math.ceil(total / pageSize)

  const formatDate = (dateString) => {
    if (!dateString) return null
    const date = new Date(dateString)
    return date.toLocaleString('pt-BR', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  return (
    <div>
      <div style={{
        display: 'flex',
        alignItems: 'flex-start',
        justifyContent: 'space-between',
        marginBottom: '1.5rem',
        gap: '1rem',
        flexWrap: 'wrap',
      }}>
        <div>
          <h1 style={{
            fontFamily: 'var(--font-display)',
            fontSize: '1.75rem',
            fontWeight: 700,
            color: 'var(--text-primary)',
            letterSpacing: '-0.02em',
          }}>
            {title}
          </h1>
          {lastUpdated && (
            <span style={{
              fontFamily: 'var(--font-display)',
              fontSize: '0.75rem',
              color: 'var(--text-muted)',
              fontWeight: 500,
              display: 'block',
              marginTop: '0.25rem',
            }}>
              Atualizado em: {formatDate(lastUpdated)}
            </span>
          )}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
          {action}
          <span style={{
            fontFamily: 'var(--font-display)',
            fontSize: '0.875rem',
            color: 'var(--text-muted)',
            fontWeight: 500,
          }}>
            {total.toLocaleString('pt-BR')} registros
          </span>
        </div>
      </div>

      <div style={{
        background: 'var(--bg-card)',
        borderRadius: 'var(--radius-lg)',
        border: '1px solid var(--border)',
        overflow: 'hidden',
        boxShadow: 'var(--shadow-md)',
        transition: 'background-color var(--transition), border-color var(--transition)',
      }}>
        <div style={{ overflowX: 'auto' }}>
          <table style={{
            width: '100%',
            borderCollapse: 'collapse',
            fontSize: '0.875rem',
          }}>
            <thead>
              <tr style={{ background: 'var(--bg-secondary)' }}>
                {columns.map(col => (
                  <th
                    key={col.key}
                    onClick={() => col.sortable !== false && handleSort(col.key)}
                    style={{
                      padding: '0.875rem 1rem',
                      textAlign: 'left',
                      fontFamily: 'var(--font-display)',
                      fontWeight: 600,
                      color: sortBy === col.key ? 'var(--orange)' : 'var(--text-secondary)',
                      borderBottom: '1px solid var(--border)',
                      cursor: col.sortable !== false ? 'pointer' : 'default',
                      userSelect: 'none',
                      whiteSpace: 'nowrap',
                      transition: 'color var(--transition)',
                    }}
                  >
                    <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                      {col.label}
                      {col.sortable !== false && sortBy === col.key && (
                        <span style={{ fontSize: '0.75rem' }}>
                          {sortDirection === 'asc' ? '↑' : '↓'}
                        </span>
                      )}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={columns.length} style={{
                    padding: '3rem',
                    textAlign: 'center',
                    color: 'var(--text-muted)',
                  }}>
                    <div style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: '0.75rem',
                    }}>
                      <div style={{
                        width: '20px',
                        height: '20px',
                        border: '2px solid var(--border)',
                        borderTopColor: 'var(--orange)',
                        borderRadius: '50%',
                        animation: 'spin 0.8s linear infinite',
                      }} />
                      Carregando...
                    </div>
                  </td>
                </tr>
              ) : error ? (
                <tr>
                  <td colSpan={columns.length} style={{
                    padding: '3rem',
                    textAlign: 'center',
                    color: '#EF4444',
                  }}>
                    Erro: {error}
                    <button
                      onClick={fetchData}
                      style={{
                        marginLeft: '1rem',
                        padding: '0.5rem 1rem',
                        background: 'var(--orange)',
                        color: 'white',
                        border: 'none',
                        borderRadius: 'var(--radius-sm)',
                        cursor: 'pointer',
                      }}
                    >
                      Tentar novamente
                    </button>
                  </td>
                </tr>
              ) : data.length === 0 ? (
                <tr>
                  <td colSpan={columns.length} style={{
                    padding: '3rem',
                    textAlign: 'center',
                    color: 'var(--text-muted)',
                  }}>
                    Nenhum dado encontrado
                  </td>
                </tr>
              ) : (
                data.map((row, idx) => (
                  <tr
                    key={idx}
                    style={{
                      background: idx % 2 === 0 ? 'transparent' : 'var(--bg-secondary)',
                      transition: 'background-color var(--transition)',
                    }}
                    onMouseEnter={e => {
                      e.currentTarget.style.background = 'rgba(249, 115, 22, 0.05)'
                    }}
                    onMouseLeave={e => {
                      e.currentTarget.style.background = idx % 2 === 0 ? 'transparent' : 'var(--bg-secondary)'
                    }}
                  >
                    {columns.map(col => (
                      <td
                        key={col.key}
                        style={{
                          padding: '0.75rem 1rem',
                          borderBottom: '1px solid var(--border)',
                          color: 'var(--text-primary)',
                        }}
                      >
                        {renderCell ? renderCell(row, col.key) : row[col.key]}
                      </td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {!loading && !error && data.length > 0 && (
          <div style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '1rem',
            borderTop: '1px solid var(--border)',
            background: 'var(--bg-secondary)',
          }}>
            <span style={{ color: 'var(--text-muted)', fontSize: '0.875rem' }}>
              Página {page} de {totalPages}
            </span>
            <div style={{ display: 'flex', gap: '0.5rem' }}>
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page === 1}
                style={{
                  padding: '0.5rem 1rem',
                  background: 'var(--bg-card)',
                  border: '1px solid var(--border)',
                  borderRadius: 'var(--radius-sm)',
                  color: page === 1 ? 'var(--text-muted)' : 'var(--text-primary)',
                  cursor: page === 1 ? 'not-allowed' : 'pointer',
                  transition: 'all var(--transition)',
                }}
              >
                Anterior
              </button>
              <button
                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                disabled={page === totalPages}
                style={{
                  padding: '0.5rem 1rem',
                  background: page === totalPages ? 'var(--bg-tertiary)' : 'var(--orange)',
                  border: 'none',
                  borderRadius: 'var(--radius-sm)',
                  color: page === totalPages ? 'var(--text-muted)' : 'white',
                  cursor: page === totalPages ? 'not-allowed' : 'pointer',
                  transition: 'all var(--transition)',
                }}
              >
                Próxima
              </button>
            </div>
          </div>
        )}
      </div>

      <style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  )
}

export default TableView

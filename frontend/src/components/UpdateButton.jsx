import { useState } from 'react'

function UpdateButton({ onSuccess }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const handleUpdate = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch('/api/update/atletas', { method: 'POST' })
      if (!res.ok) {
        const data = await res.json()
        throw new Error(data.detail || 'Failed to update')
      }
      const data = await res.json()
      if (onSuccess) onSuccess(data.updated_at)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
      <button
        onClick={handleUpdate}
        disabled={loading}
        style={{
          padding: '0.5rem 1rem',
          background: loading ? 'var(--bg-tertiary)' : 'var(--orange)',
          border: 'none',
          borderRadius: 'var(--radius-sm)',
          color: loading ? 'var(--text-muted)' : 'white',
          fontWeight: 600,
          fontSize: '0.875rem',
          cursor: loading ? 'not-allowed' : 'pointer',
          transition: 'all var(--transition)',
          display: 'flex',
          alignItems: 'center',
          gap: '0.5rem',
        }}
      >
        {loading && (
          <div style={{
            width: '14px',
            height: '14px',
            border: '2px solid rgba(255,255,255,0.3)',
            borderTopColor: 'white',
            borderRadius: '50%',
            animation: 'spin 0.8s linear infinite',
          }} />
        )}
        {loading ? 'Atualizando...' : 'Atualizar Atletas'}
      </button>
      {error && (
        <span style={{ color: '#EF4444', fontSize: '0.75rem' }}>
          Erro: {error}
        </span>
      )}
      <style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  )
}

export default UpdateButton
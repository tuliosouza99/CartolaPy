import { useState, useEffect } from 'react'
import TableView from '../components/TableView'

function DevDashboard() {
  const [activeTab, setActiveTab] = useState('atletas')
  const [redisData, setRedisData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchRedisData()
  }, [])

  const fetchRedisData = async () => {
    try {
      const res = await fetch('/api/redis/all')
      if (res.ok) {
        const data = await res.json()
        setRedisData(data)
      }
    } catch (err) {
      console.error('Failed to fetch redis data:', err)
    } finally {
      setLoading(false)
    }
  }

  const tabs = [
    { key: 'atletas', label: 'Atletas' },
    { key: 'pontuacoes', label: 'Pontuações' },
    { key: 'confrontos', label: 'Confrontos' },
    { key: 'pontos-cedidos', label: 'Pontos Cedidos' },
    { key: 'redis', label: 'Redis' },
  ]

  const atletasColumns = [
    { key: 'atleta_id', label: 'ID' },
    { key: 'rodada_id', label: 'Rodada' },
    { key: 'clube_id', label: 'Clube ID' },
    { key: 'posicao_id', label: 'Posição' },
    { key: 'status_id', label: 'Status' },
    { key: 'apelido', label: 'Apelido' },
    { key: 'preco_num', label: 'Preço' },
  ]

  const pontuacoesColumns = [
    { key: 'atleta_id', label: 'Atleta ID' },
    { key: 'rodada_id', label: 'Rodada' },
    { key: 'clube_id', label: 'Clube ID' },
    { key: 'posicao_id', label: 'Posição' },
    { key: 'pontuacao', label: 'Pontuação' },
    { key: 'pontuacao_basica', label: 'PB' },
  ]

  const confrontosColumns = [
    { key: 'clube_id', label: 'Clube ID' },
    { key: 'opponent_clube_id', label: 'Adversário' },
    { key: 'rodada_id', label: 'Rodada' },
    { key: 'is_mandante', label: 'Mandante' },
  ]

  const pontosCedidosColumns = [
    { key: 'clube_id', label: 'Clube ID' },
    { key: 'posicao_id', label: 'Posição' },
    { key: 'rodada_id', label: 'Rodada' },
    { key: 'is_mandante', label: 'Mandante' },
    { key: 'pontuacao', label: 'Pontos Cedidos' },
    { key: 'pontuacao_basica', label: 'PB' },
    { key: 'G', label: 'G' },
    { key: 'A', label: 'A' },
    { key: 'FT', label: 'FT' },
    { key: 'FD', label: 'FD' },
    { key: 'FF', label: 'FF' },
    { key: 'FS', label: 'FS' },
    { key: 'PS', label: 'PS' },
    { key: 'V', label: 'V' },
    { key: 'I', label: 'I' },
    { key: 'PP', label: 'PP' },
    { key: 'DS', label: 'DS' },
    { key: 'SG', label: 'SG' },
    { key: 'DE', label: 'DE' },
    { key: 'DP', label: 'DP' },
    { key: 'CV', label: 'CV' },
    { key: 'CA', label: 'CA' },
    { key: 'FC', label: 'FC' },
    { key: 'GC', label: 'GC' },
    { key: 'GS', label: 'GS' },
    { key: 'PC', label: 'PC' },
  ]

  const getEndpoint = () => {
    switch (activeTab) {
      case 'atletas': return 'atletas'
      case 'pontuacoes': return 'pontuacoes'
      case 'confrontos': return 'confrontos'
      case 'pontos-cedidos': return 'pontos-cedidos'
      default: return null
    }
  }

  const getColumns = () => {
    switch (activeTab) {
      case 'atletas': return atletasColumns
      case 'pontuacoes': return pontuacoesColumns
      case 'confrontos': return confrontosColumns
      case 'pontos-cedidos': return pontosCedidosColumns
      default: return []
    }
  }

  return (
    <div>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: '1rem',
        marginBottom: '1.5rem',
        padding: '1rem',
        background: 'var(--bg-card)',
        borderRadius: 'var(--radius-lg)',
        border: '2px solid var(--orange)',
      }}>
        <span style={{
          fontFamily: 'var(--font-display)',
          fontSize: '0.875rem',
          fontWeight: 600,
          color: 'var(--orange)',
        }}>
          DEV MODE
        </span>
        <span style={{
          fontFamily: 'var(--font-display)',
          fontSize: '0.75rem',
          color: 'var(--text-muted)',
        }}>
          Raw data access - not for production
        </span>
      </div>

      <div style={{
        display: 'flex',
        gap: '0.5rem',
        marginBottom: '1.5rem',
        borderBottom: '1px solid var(--border)',
        paddingBottom: '0.5rem',
      }}>
        {tabs.map(tab => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            style={{
              padding: '0.5rem 1rem',
              background: activeTab === tab.key ? 'var(--orange)' : 'transparent',
              color: activeTab === tab.key ? 'white' : 'var(--text-secondary)',
              border: 'none',
              borderRadius: 'var(--radius-sm)',
              cursor: 'pointer',
              fontFamily: 'var(--font-display)',
              fontSize: '0.875rem',
              fontWeight: 500,
              transition: 'all var(--transition)',
            }}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === 'redis' ? (
        <div style={{
          background: 'var(--bg-card)',
          borderRadius: 'var(--radius-lg)',
          border: '1px solid var(--border)',
          padding: '1rem',
          fontFamily: 'var(--font-mono)',
          fontSize: '0.75rem',
          overflow: 'auto',
          maxHeight: '600px',
        }}>
          {loading ? (
            <div style={{ color: 'var(--text-muted)' }}>Loading...</div>
          ) : redisData ? (
            <pre style={{ margin: 0, whiteSpace: 'pre-wrap', color: 'var(--text-primary)' }}>
              {JSON.stringify(redisData, null, 2)}
            </pre>
          ) : (
            <div style={{ color: 'var(--text-muted)' }}>Failed to load Redis data</div>
          )}
        </div>
      ) : (
        <TableView
          title={tabs.find(t => t.key === activeTab)?.label || ''}
          endpoint={getEndpoint()}
          columns={getColumns()}
          hideUpdate={false}
          hideCount={false}
          hideTimestamps={false}
        />
      )}
    </div>
  )
}

export default DevDashboard
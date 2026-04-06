import { useState, useEffect, useMemo, useCallback } from 'react'
import TableView from '../components/TableView'
import UpdateButton from '../components/UpdateButton'
import RoundIntervalSlider from '../components/RoundIntervalSlider'
import MandoToggle from '../components/MandoToggle'

const STATUS_COLORS = {
  green: '#22c55e',
  yellow: '#eab308',
  red: '#ef4444',
}

function AtletasUnified() {
  const [statusData, setStatusData] = useState(null)
  const [rodadaRange, setRodadaRange] = useState({ min: 1, max: 1 })
  const [isMandante, setIsMandante] = useState('geral')

  useEffect(() => {
    fetchStatus()
  }, [])

  const fetchStatus = async () => {
    try {
      const res = await fetch('/api/tables/status')
      if (res.ok) {
        const data = await res.json()
        setStatusData(data)
        setRodadaRange({ min: 1, max: data.rodada_atual || 1 })
      }
    } catch (err) {
      console.error('Failed to fetch status:', err)
    }
  }

  const lastUpdatedMap = useMemo(() => {
    if (!statusData) return {}
    return {
      atletas: statusData.atletas,
      pontuacoes: statusData.pontuacoes,
      confrontos: statusData.confrontos,
    }
  }, [statusData])

  const columns = useMemo(() => [
    { key: 'apelido', label: 'Atleta', sortable: true },
    {
      key: 'clube_escudo',
      label: 'Clube',
      sortable: false,
      renderCell: (row) => (
        <img
          src={row.clube_escudo}
          alt="clube"
          style={{ width: '24px', height: '24px', objectFit: 'contain' }}
          onError={(e) => { e.target.style.display = 'none' }}
        />
      ),
    },
    { key: 'posicao_abreviacao', label: 'Posição', sortable: true },
    {
      key: 'status_nome',
      label: 'Status',
      sortable: true,
      renderCell: (row) => (
        <span style={{
          color: STATUS_COLORS[row.status_cor] || STATUS_COLORS.red,
          fontWeight: 500,
        }}>
          {row.status_nome}
        </span>
      ),
    },
    {
      key: 'preco',
      label: 'Preço',
      sortable: true,
      renderCell: (row) => String(row.preco || ''),
    },
    { key: 'media', label: 'Média', sortable: true },
    { key: 'media_basica', label: 'Média Básica', sortable: true },
    { key: 'total_jogos', label: 'Total de Jogos', sortable: true },
    {
      key: 'proximo_jogo',
      label: 'Próximo Jogo',
      sortable: false,
      renderCell: (row) => {
        const jogo = row.proximo_jogo
        if (!jogo || Object.keys(jogo).length === 0) {
          return <span style={{ color: 'var(--text-muted)' }}>-</span>
        }
        const hasMandante = Boolean(jogo.mandante_escudo)
        const hasVisitante = Boolean(jogo.visitante_escudo)
        if (!hasMandante && !hasVisitante) {
          return <span style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>-</span>
        }
        return (
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
            {hasMandante ? (
              <img
                src={jogo.mandante_escudo}
                alt="mandante"
                style={{ width: '20px', height: '20px', objectFit: 'contain' }}
                onError={(e) => { e.target.style.display = 'none' }}
              />
            ) : (
              <span style={{ width: '20px', height: '20px' }} />
            )}
            <span style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>x</span>
            {hasVisitante ? (
              <img
                src={jogo.visitante_escudo}
                alt="visitante"
                style={{ width: '20px', height: '20px', objectFit: 'contain' }}
                onError={(e) => { e.target.style.display = 'none' }}
              />
            ) : (
              <span style={{ width: '20px', height: '20px' }} />
            )}
          </div>
        )
      },
    },
  ], [])

  const expandedContent = useCallback((row) => {
    const scouts = row.scouts || {}
    const scoutEntries = Object.entries(scouts).filter(([_, value]) => value > 0)

    if (scoutEntries.length === 0) {
      return (
        <div style={{ padding: '1rem', color: 'var(--text-muted)' }}>
          Nenhum scout registrado
        </div>
      )
    }

    return (
      <div style={{ padding: '1rem' }}>
        <div style={{
          fontFamily: 'var(--font-display)',
          fontSize: '0.75rem',
          fontWeight: 600,
          color: 'var(--text-secondary)',
          marginBottom: '0.5rem',
          textTransform: 'uppercase',
          letterSpacing: '0.05em',
        }}>
          Scouts
        </div>
        <div style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: '0.5rem',
        }}>
          {scoutEntries.map(([key, value]) => (
            <span
              key={key}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '0.25rem',
                padding: '0.25rem 0.5rem',
                background: 'var(--bg-card)',
                borderRadius: 'var(--radius-sm)',
                fontFamily: 'var(--font-mono)',
                fontSize: '0.75rem',
                color: 'var(--text-primary)',
              }}
            >
              <span style={{ fontWeight: 600 }}>{key}</span>
              <span style={{ color: 'var(--orange)' }}>{value}</span>
            </span>
          ))}
        </div>
      </div>
    )
  }, [])

  const filterComponent = useMemo(() => (
    <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
      <RoundIntervalSlider
        min={1}
        max={statusData?.rodada_atual || 1}
        value={rodadaRange}
        onChange={setRodadaRange}
      />
      <MandoToggle
        value={isMandante}
        onChange={setIsMandante}
      />
    </div>
  ), [rodadaRange, isMandante, statusData?.rodada_atual])

  const extraParams = useMemo(() => ({
    rodada_min: rodadaRange.min,
    rodada_max: rodadaRange.max,
    is_mandante: isMandante,
  }), [rodadaRange, isMandante])

  return (
    <div>
      <TableView
        title="Atletas"
        endpoint="atletas-unified"
        columns={columns}
        lastUpdatedMap={lastUpdatedMap}
        action={<UpdateButton onSuccess={fetchStatus} />}
        filterComponent={filterComponent}
        extraParams={extraParams}
        expandable={true}
        expandedContent={expandedContent}
      />
    </div>
  )
}

export default AtletasUnified

import { useState, useEffect, useMemo } from 'react'
import TableView from '../components/TableView'

const columns = [
  { key: 'atleta_id', label: 'Atleta ID' },
  { key: 'apelido', label: 'Apelido' },
  { key: 'rodada_id', label: 'Rodada' },
  { key: 'clube_id', label: 'Clube ID' },
  { key: 'posicao_id', label: 'Posição' },
  { 
    key: 'pontuacao', 
    label: 'Pontuação',
    renderCell: (row, key) => {
      const val = row[key]
      if (val == null) return '-'
      const num = Number(val)
      const color = num >= 10 ? '#22C55E' : num >= 5 ? 'var(--orange)' : num < 0 ? '#EF4444' : 'var(--text-primary)'
      return <span style={{ color, fontWeight: 600 }}>{num.toFixed(1)}</span>
    }
  },
  { 
    key: 'pontuacao_basica', 
    label: 'Pontuação Básica',
    renderCell: (row, key) => {
      const val = row[key]
      return val != null ? Number(val).toFixed(1) : '-'
    }
  },
]

function Pontuacoes() {
  const [statusData, setStatusData] = useState(null)

  useEffect(() => {
    fetchStatus()
  }, [])

  const fetchStatus = async () => {
    try {
      const res = await fetch('/api/tables/status')
      if (res.ok) {
        const data = await res.json()
        setStatusData(data)
      }
    } catch (err) {
      console.error('Failed to fetch status:', err)
    }
  }

  const lastUpdatedMap = useMemo(() => {
    if (!statusData) return {}
    return { pontuacoes: statusData.pontuacoes }
  }, [statusData])

  return (
    <TableView 
      title="Pontuações" 
      endpoint="pontuacoes" 
      columns={columns} 
      lastUpdatedMap={lastUpdatedMap}
    />
  )
}

export default Pontuacoes
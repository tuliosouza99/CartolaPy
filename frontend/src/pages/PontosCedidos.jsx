import { useState, useEffect, useMemo } from 'react'
import TableView from '../components/TableView'

const columns = [
  { key: 'clube_id', label: 'Clube' },
  { key: 'rodada_id', label: 'Rodada' },
  { key: 'posicao_id', label: 'Posição' },
  { 
    key: 'is_mandante', 
    label: 'Mandante',
    renderCell: (row, key) => {
      const val = row[key]
      return val ? 'Sim' : 'Não'
    }
  },
  { 
    key: 'pontuacao', 
    label: 'Pontos Cedidos',
    renderCell: (row, key) => {
      const val = row[key]
      if (val == null) return '-'
      const num = Number(val)
      return <span style={{ color: num > 5 ? '#EF4444' : 'var(--text-primary)' }}>
        {num.toFixed(1)}
      </span>
    }
  },
  { 
    key: 'pontuacao_basica', 
    label: 'PB',
    renderCell: (row, key) => {
      const val = row[key]
      return val != null ? Number(val).toFixed(1) : '-'
    }
  },
]

function PontosCedidos() {
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
    return { pontos_cedidos: statusData.pontos_cedidos }
  }, [statusData])

  return (
    <TableView 
      title="Pontos Cedidos" 
      endpoint="pontos-cedidos" 
      columns={columns} 
      lastUpdatedMap={lastUpdatedMap}
    />
  )
}

export default PontosCedidos
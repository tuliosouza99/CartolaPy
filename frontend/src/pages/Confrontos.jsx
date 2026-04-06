import { useState, useEffect, useMemo } from 'react'
import TableView from '../components/TableView'

const columns = [
  { key: 'rodada_id', label: 'Rodada' },
  { key: 'clube_id', label: 'Clube' },
  { key: 'opponent_clube_id', label: 'Adversário' },
  { 
    key: 'is_mandante', 
    label: 'Mandante',
    renderCell: (row, key) => {
      const val = row[key]
      return val ? 'Sim' : 'Não'
    }
  },
]

function Confrontos() {
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
    return { confrontos: statusData.confrontos }
  }, [statusData])

  return (
    <TableView 
      title="Confrontos" 
      endpoint="confrontos" 
      columns={columns} 
      lastUpdatedMap={lastUpdatedMap}
    />
  )
}

export default Confrontos
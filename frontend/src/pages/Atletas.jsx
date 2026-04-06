import { useState, useEffect, useMemo } from 'react'
import TableView from '../components/TableView'
import UpdateButton from '../components/UpdateButton'

const columns = [
  { key: 'atleta_id', label: 'ID' },
  { key: 'apelido', label: 'Apelido' },
  { key: 'rodada_id', label: 'Rodada' },
  { key: 'clube_id', label: 'Clube ID' },
  { key: 'posicao_id', label: 'Posição ID' },
  { key: 'status_id', label: 'Status ID' },
  { 
    key: 'preco_num', 
    label: 'Preço',
    renderCell: (row, key) => {
      const val = row[key]
      return val != null ? `R$ ${Number(val).toFixed(2)}` : '-'
    }
  },
]

function Atletas() {
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
    return { atletas: statusData.atletas }
  }, [statusData])

  return (
    <TableView 
      title="Atletas" 
      endpoint="atletas" 
      columns={columns} 
      lastUpdatedMap={lastUpdatedMap}
      action={<UpdateButton onSuccess={fetchStatus} />}
    />
  )
}

export default Atletas
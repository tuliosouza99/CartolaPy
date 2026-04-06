import { useState, useEffect, createContext, useContext } from 'react'
import { Routes, Route, Navigate } from 'react-router-dom'
import Navbar from './components/Navbar'
import Atletas from './pages/Atletas'
import AtletasUnified from './pages/AtletasUnified'
import Pontuacoes from './pages/Pontuacoes'
import Confrontos from './pages/Confrontos'
import PontosCedidos from './pages/PontosCedidos'

export const ThemeContext = createContext()

export const useTheme = () => useContext(ThemeContext)

function App() {
  const [theme, setTheme] = useState(() => {
    const saved = localStorage.getItem('cartolapy-theme')
    if (saved) return saved
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
  })

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem('cartolapy-theme', theme)
  }, [theme])

  const toggleTheme = () => setTheme(prev => prev === 'dark' ? 'light' : 'dark')

  return (
    <ThemeContext.Provider value={{ theme, toggleTheme }}>
      <div style={{ 
        minHeight: '100vh', 
        background: 'var(--bg-primary)',
        transition: 'background-color var(--transition)'
      }}>
        <Navbar />
        <main style={{ 
          flex: 1,
          padding: '2rem',
          maxWidth: '1400px',
          margin: '0 auto',
          width: '100%'
        }}>
          <Routes>
            <Route path="/" element={<Navigate to="/atletas" replace />} />
            <Route path="/atletas" element={<Atletas />} />
            <Route path="/atletas-unified" element={<AtletasUnified />} />
            <Route path="/pontuacoes" element={<Pontuacoes />} />
            <Route path="/confrontos" element={<Confrontos />} />
            <Route path="/pontos-cedidos" element={<PontosCedidos />} />
          </Routes>
        </main>
      </div>
    </ThemeContext.Provider>
  )
}

export default App

import React, { useEffect } from 'react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Navbar from './components/Navbar'
import Dashboard from './pages/Dashboard'
import StaticAnalytics from './pages/StaticAnalytics'
import { ThemeProvider } from './contexts/ThemeContext'

function App() {
  return (
    <ThemeProvider>
      <Router>
        <div className="min-h-screen">
          <Navbar />          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/static-analytics" element={<StaticAnalytics />} />
          </Routes>
        </div>
      </Router>
    </ThemeProvider>
  )
}

export default App

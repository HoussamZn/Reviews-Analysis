import React, { createContext, useState, useEffect, useContext } from 'react';
import config from '../config';

// Create theme context
const ThemeContext = createContext();

// Create a provider component
export const ThemeProvider = ({ children }) => {  // Initialize theme from localStorage or use the default from config
  const [theme, setTheme] = useState(() => {
    const savedTheme = localStorage.getItem('theme');
    return savedTheme || config.ui.theme;
  });

  // Set initial theme classes when component mounts
  useEffect(() => {
    // Apply initial theme class
    if (theme === 'dark') {
      document.documentElement.classList.add('dark');
      document.documentElement.classList.add('dark-theme');
      document.documentElement.classList.remove('light-theme');
    } else {
      document.documentElement.classList.remove('dark');
      document.documentElement.classList.remove('dark-theme');
      document.documentElement.classList.add('light-theme');
    }
  }, []);

  // Update localStorage and document class when theme changes
  useEffect(() => {
    localStorage.setItem('theme', theme);
    
    // Update config value
    config.ui.theme = theme;
    
    // Update document class for Tailwind dark mode
    if (theme === 'dark') {
      document.documentElement.classList.add('dark');
      document.documentElement.classList.add('dark-theme');
      document.documentElement.classList.remove('light-theme');
    } else {
      document.documentElement.classList.remove('dark');
      document.documentElement.classList.remove('dark-theme');
      document.documentElement.classList.add('light-theme');
    }
  }, [theme]);

  // Toggle theme between light and dark
  const toggleTheme = () => {
    setTheme(prevTheme => prevTheme === 'light' ? 'dark' : 'light');
  };

  return (
    <ThemeContext.Provider value={{ theme, toggleTheme }}>
      {children}
    </ThemeContext.Provider>
  );
};

// Custom hook to use the theme context
export function useTheme() {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
}

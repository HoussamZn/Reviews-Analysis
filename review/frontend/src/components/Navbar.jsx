import React, { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import config from '../config';
import { useTheme } from '../contexts/ThemeContext';

const Navbar = () => {
  const location = useLocation();
  const [isOpen, setIsOpen] = useState(false);
  const { theme, toggleTheme } = useTheme();

  const toggleMobileMenu = () => {
    setIsOpen(!isOpen);
  };

  return (
    <nav className="bg-primary-darkBlue text-white shadow-lg">
      <div className="container mx-auto px-4">
        <div className="flex justify-between items-center py-4">
          {/* Logo and title */}
          <div className="flex items-center space-x-2">
            <div className="w-10 h-10 rounded-full bg-gradient-blue-purple flex items-center justify-center">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-6 w-6"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                />
              </svg>
            </div>
            <h1 className="text-xl font-bold">Review Analytics</h1>
          </div>          {/* Desktop navigation */}          <div className="hidden md:flex space-x-6">
            <Link
              to="/"
              className={`nav-link ${
                location.pathname === '/'
                  ? 'text-secondary-teal font-semibold'
                  : 'text-white dark:text-white'
              }`}
            >
              Data Stream
            </Link>
            <Link
              to="/static-analytics"
              className={`nav-link ${
                location.pathname === '/static-analytics'
                  ? 'text-secondary-teal font-semibold'
                  : 'text-white dark:text-white'
              }`}
            >
              Data Analytics
            </Link>
          </div>

          {/* Theme toggle and profile - for desktop */}
          <div className="hidden md:flex items-center space-x-4">
            <button
              onClick={toggleTheme}
              className="px-3 py-1 rounded-full hover:bg-primary-mediumBlue hover:bg-opacity-20 flex items-center space-x-2"
              aria-label="Toggle theme"
            >
              {theme === 'dark' ? (
                <>
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-5 w-5"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
                    />
                  </svg>
                  <span className="text-xs">Light Mode</span>
                </>
              ) : (
                <>
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-5 w-5"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"
                    />
                  </svg>
                  <span className="text-xs">Dark Mode</span>
                </>
              )}
            </button>
          </div>

          {/* Mobile menu button */}
          <div className="md:hidden">
            <button onClick={toggleMobileMenu} className="text-white">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-6 w-6"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d={
                    isOpen
                      ? 'M6 18L18 6M6 6l12 12'
                      : 'M4 6h16M4 12h16M4 18h16'
                  }
                />
              </svg>
            </button>
          </div>
        </div>

        {/* Mobile menu */}
        {isOpen && (
          <div className="md:hidden py-4 border-t border-primary-mediumBlue border-opacity-30">            <Link
              to="/"
              className={`block py-2 ${
                location.pathname === '/'
                  ? 'text-secondary-teal font-semibold'
                  : 'text-white dark:text-white'
              }`}
              onClick={() => setIsOpen(false)}
            >
              Dashboard
            </Link>
            <Link
              to="/static-analytics"
              className={`block py-2 ${
                location.pathname === '/static-analytics'
                  ? 'text-secondary-teal font-semibold'
                  : 'text-white dark:text-white'
              }`}
              onClick={() => setIsOpen(false)}
            >
              Static Analytics
            </Link>
            <div className="flex items-center space-x-4 mt-4">
              <button 
                onClick={toggleTheme}
                className="px-3 py-1 rounded-full hover:bg-primary-mediumBlue hover:bg-opacity-20 flex items-center space-x-2">
                {theme === 'dark' ? (
                  <>
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      className="h-5 w-5"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
                      />
                    </svg>
                    <span className="text-sm">Light Mode</span>
                  </>
                ) : (
                  <>
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      className="h-5 w-5"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"
                      />
                    </svg>
                    <span className="text-sm">Dark Mode</span>
                  </>
                )}
              </button>
            </div>
          </div>
        )}
      </div>
    </nav>
  );
};

export default Navbar;

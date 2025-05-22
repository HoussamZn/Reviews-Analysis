// Dashboard filters component
import React, { useState } from 'react';

const DashboardFilters = ({ onFilterChange }) => {
  const [filters, setFilters] = useState({
    sentiment: 'all',
    rating: 'all',
    dateRange: '7days'
  });

  const handleFilterChange = (filterType, value) => {
    const updatedFilters = {
      ...filters,
      [filterType]: value
    };
    
    setFilters(updatedFilters);
    
    // Pass the updated filters to parent component
    if (onFilterChange) {
      onFilterChange(updatedFilters);
    }
  };
  return (
    <div className="dashboard-card bg-white dark:bg-background-darkNavy p-5 mb-6">
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-lg font-semibold text-primary-darkBlue dark:text-primary-brightBlue">Filters</h3>
        <button
          type="button"
          onClick={() => {
            const defaultFilters = { sentiment: 'all', rating: 'all', dateRange: '7days' };
            setFilters(defaultFilters);
            if (onFilterChange) {
              onFilterChange(defaultFilters);
            }
          }}
          className="text-sm text-primary-mediumBlue hover:text-primary-brightBlue transition-colors px-3 py-1 rounded-md hover:bg-blue-50 dark:hover:bg-primary-darkBlue"
        >
          Reset Filters
        </button>
      </div>
      
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Sentiment Filter */}
        <div>
          <label htmlFor="sentiment-filter" className="block text-sm font-medium text-primary-darkBlue dark:text-primary-brightBlue mb-2">
            Sentiment
          </label>
          <div className="relative">            <select
              id="sentiment-filter"
              value={filters.sentiment}
              onChange={(e) => handleFilterChange('sentiment', e.target.value)}
              className="block w-full rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-background-darkNavy focus:ring-primary-mediumBlue focus:border-primary-mediumBlue pr-10 py-2.5 text-sm dark:text-white appearance-none cursor-pointer custom-select"
            >
              <option value="all">All Sentiments</option>
              <option value="positive">Positive</option>
              <option value="neutral">Neutral</option>
              <option value="negative">Negative</option>
            </select>
            <div className="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
              <div className="w-5 h-5 rounded-full bg-primary-mediumBlue dark:bg-primary-brightBlue flex items-center justify-center">
                <svg className="w-3 h-3 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </div>
            </div>
          </div>
        </div>
          {/* Rating Filter */}
        <div>
          <label htmlFor="rating-filter" className="block text-sm font-medium text-primary-darkBlue dark:text-primary-brightBlue mb-2">
            Rating
          </label>
          <div className="relative">            <select
              id="rating-filter"
              value={filters.rating}
              onChange={(e) => handleFilterChange('rating', e.target.value)}
              className="block w-full rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-background-darkNavy focus:ring-primary-mediumBlue focus:border-primary-mediumBlue pr-10 py-2.5 text-sm dark:text-white appearance-none cursor-pointer custom-select"
            >
              <option value="all">All Ratings</option>
              <option value="5">5 Stars</option>
              <option value="4">4 Stars</option>
              <option value="3">3 Stars</option>
              <option value="2">2 Stars</option>
              <option value="1">1 Star</option>
            </select>
            <div className="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
              <div className="w-5 h-5 rounded-full bg-primary-mediumBlue dark:bg-primary-brightBlue flex items-center justify-center">
                <svg className="w-3 h-3 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </div>
            </div>
          </div>
        </div>
          {/* Date Range Filter */}
        <div>
          <label htmlFor="date-filter" className="block text-sm font-medium text-primary-darkBlue dark:text-primary-brightBlue mb-2">
            Date Range
          </label>
          <div className="relative">            <select
              id="date-filter"
              value={filters.dateRange}
              onChange={(e) => handleFilterChange('dateRange', e.target.value)}
              className="block w-full rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-background-darkNavy focus:ring-primary-mediumBlue focus:border-primary-mediumBlue pr-10 py-2.5 text-sm dark:text-white appearance-none cursor-pointer custom-select"
            >
              <option value="7days">Last 7 Days</option>
              <option value="30days">Last 30 Days</option>
              <option value="90days">Last 90 Days</option>
              <option value="all">All Time</option>
            </select>
            <div className="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
              <div className="w-5 h-5 rounded-full bg-primary-mediumBlue dark:bg-primary-brightBlue flex items-center justify-center">
                <svg className="w-3 h-3 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DashboardFilters;

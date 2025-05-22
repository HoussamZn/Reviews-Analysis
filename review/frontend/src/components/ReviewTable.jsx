import React, { useState } from 'react';
import config from '../config';
import { useTheme } from '../contexts/ThemeContext';

const ReviewTable = ({ reviews }) => {
  const [sortConfig, setSortConfig] = useState({
    key: 'reviewTime',
    direction: 'desc'
  });
  const { theme } = useTheme();
  
  if (!reviews || !Array.isArray(reviews)) {
    return (
      <div className="bg-red-50 border border-secondary-red rounded-lg p-6">
        <p className="text-secondary-red">Error: No reviews available</p>
      </div>
    );
  }
  if (reviews.length === 0) {
    return (
      <div className="bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-6">
        <p className="text-gray-500 dark:text-gray-400">No reviews yet</p>
      </div>
    );
  }

  // Sort reviews based on current sort configuration
  const sortedReviews = [...reviews].sort((a, b) => {
    if (a[sortConfig.key] < b[sortConfig.key]) {
      return sortConfig.direction === 'asc' ? -1 : 1;
    }
    if (a[sortConfig.key] > b[sortConfig.key]) {
      return sortConfig.direction === 'asc' ? 1 : -1;
    }
    return 0;
  });

  const requestSort = (key) => {
    let direction = 'asc';
    if (sortConfig.key === key && sortConfig.direction === 'asc') {
      direction = 'desc';
    }
    setSortConfig({ key, direction });
  };

  const getSortIndicator = (key) => {
    if (sortConfig.key !== key) return null;
    return sortConfig.direction === 'asc' ? '↑' : '↓';
  };

  const truncateText = (text, maxLength = 100) => {
    return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
  };

  // Format date for better readability
  const formatDate = (dateString) => {
    try {
      const date = new Date(dateString);
      return new Intl.DateTimeFormat('en-US', { 
        month: 'short', 
        day: 'numeric', 
        year: 'numeric' 
      }).format(date);
    } catch (e) {
      return dateString;
    }
  };

  // Get sentiment style based on prediction
  const getSentimentStyle = (sentiment) => {
    switch (sentiment) {
      case 'positive':
        return 'bg-opacity-15 bg-secondary-teal text-secondary-teal';
      case 'negative':
        return 'bg-opacity-15 bg-secondary-red text-secondary-red';
      default:
        return 'bg-opacity-15 bg-primary-lightBlue text-primary-brightBlue';
    }
  };

  return (
    <div className="dashboard-card overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full">          <thead>
            <tr className="bg-gray-50 dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
              <th 
                className="px-6 py-3 text-left text-xs font-medium text-primary-darkBlue dark:text-primary-brightBlue uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
                onClick={() => requestSort('reviewTime')}
              >
                Date {getSortIndicator('reviewTime')}
              </th>
              <th 
                className="px-6 py-3 text-left text-xs font-medium text-primary-darkBlue dark:text-primary-brightBlue uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
                onClick={() => requestSort('overall')}
              >
                Rating {getSortIndicator('overall')}
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-primary-darkBlue dark:text-primary-brightBlue uppercase tracking-wider">
                Review
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-primary-darkBlue dark:text-primary-brightBlue uppercase tracking-wider">
                Product ID
              </th>
              <th 
                className="px-6 py-3 text-left text-xs font-medium text-primary-darkBlue dark:text-primary-brightBlue uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
                onClick={() => requestSort('prediction')}
              >
                Sentiment {getSortIndicator('prediction')}
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-primary-darkBlue dark:text-primary-brightBlue uppercase tracking-wider">
                Helpful
              </th>
            </tr>
          </thead>          <tbody className="divide-y divide-gray-200 dark:divide-gray-700 bg-white dark:bg-gray-900">
            {sortedReviews.map((review, index) => (
              <tr key={`${review.reviewerID}-${index}`} className="hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                  {formatDate(review.reviewTime)}
                </td>
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="flex">
                    {Array.from({ length: 5 }).map((_, i) => (
                      <svg 
                        key={i}
                        className={`w-4 h-4 ${i < Math.round(review.overall) ? 'text-primary-brightBlue' : 'text-gray-300 dark:text-gray-600'}`}
                        fill="currentColor"
                        viewBox="0 0 20 20"
                        xmlns="http://www.w3.org/2000/svg"
                      >
                        <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                      </svg>
                    ))}
                  </div>
                </td>
                <td className="px-6 py-4 text-sm text-gray-900 dark:text-gray-300">
                  <div className="max-w-xs">{truncateText(review.reviewText)}</div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                  {review.asin}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm">
                  <span className={`px-2 py-1 rounded-full text-xs font-medium ${getSentimentStyle(review.prediction)}`}>
                    {review.prediction}
                  </span>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                  {review.helpful[0]}/{review.helpful[1]}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default ReviewTable;

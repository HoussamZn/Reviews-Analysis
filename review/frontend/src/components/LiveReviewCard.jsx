import React from 'react';
import config from '../config';
import { useTheme } from '../contexts/ThemeContext';

const LiveReviewCard = ({ review }) => {
  const { theme } = useTheme();
  
  // Add error handling for missing review data
  if (!review) {
    return (
      <div className="bg-red-50 border border-secondary-red rounded-lg p-6 mb-4">
        <p className="text-secondary-red">Error: Review data is missing</p>
      </div>
    );
  }

  // Destructure review with default values to prevent undefined errors
  console.log(review);
  const {
    reviewerName = 'Anonymous',
    unixReviewTime = 'No date',
    prediction = 'neutral',
    reviewText = 'No review text',
    asin = 'Unknown',
    processing_time = 0
  } = review;
  const date = new Date(parseInt(unixReviewTime) * 1000);
  const formatted = date.toString().split(' GMT')[0];

  // Get sentiment color based on prediction
  const getSentimentColor = (sentiment) => {
    switch (sentiment) {
      case 'Positive':
        return 'bg-secondary-teal text-background-darkNavy';
      case 'Negative':
        return 'bg-secondary-red text-white';
      default:
        return 'bg-primary-lightBlue text-background-darkNavy';
    }
  };

  // Format date to be more readable
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
  return (
    <div className="dashboard-card p-6 mb-4 hover:shadow-xl transition-all duration-300 border-t-4" 
         style={{ borderTopColor: config.ui.sentimentColors[prediction] }}>
      <div className="flex flex-col md:flex-row justify-between items-start mb-4">
        <div>
          <div className="flex items-center">
            <div className="w-10 h-10 rounded-full bg-gradient-blue-purple flex items-center justify-center text-white font-medium">
              {reviewerName[0]}
            </div>
            <div className="ml-3">              <h3 className="text-lg font-semibold text-primary-darkBlue dark:text-primary-brightBlue">{reviewerName}</h3>
              <p className="text-gray-500 dark:text-gray-400 text-sm">{formatted}</p>            </div>
          </div>
        </div>
        <div className="flex flex-col items-end mt-2 md:mt-0">
          <span className={`mt-2 text-sm px-3 py-1 rounded-full ${getSentimentColor(prediction)}`}>
            {prediction}
          </span>
        </div>
      </div>      <p className="text-gray-700 dark:text-gray-300 mb-4">{reviewText}</p>      <div className="flex flex-wrap justify-between text-sm text-gray-500 dark:text-gray-400 border-t pt-3 mt-2 border-gray-100 dark:border-gray-700">
        <span className="flex items-center mb-2 md:mb-0">
          <svg className="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 11V7a4 4 0 00-8 0v4M5 9h14l1 12H4L5 9z" />
          </svg>
          Product ASIN : {asin}
        </span>
        <div className="flex items-center space-x-4">
          <span className="flex items-center text-primary-mediumBlue font-medium">
            <svg className="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            {formatDate(processing_time)}
          </span>
        </div>
      </div>
    </div>
  );
};

export default LiveReviewCard;

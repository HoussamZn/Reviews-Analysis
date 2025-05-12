CREATE KEYSPACE IF NOT EXISTS review_analysis
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS review_analysis.product_reviews (
    id UUID PRIMARY KEY,
    asin TEXT,
    review_text TEXT,
    prediction TEXT,
    processing_time TIMESTAMP
);

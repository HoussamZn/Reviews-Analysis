docker exec review-cassandra-1 cqlsh -e "COPY review_analysis.product_reviews TO '/tmp/data.csv' WITH HEADER=TRUE"
docker cp review-cassandra-1:/tmp/data.csv .\Data\data.csv

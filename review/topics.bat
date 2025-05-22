docker exec -it review-kafka-1 kafka-topics --create --topic product-reviews --partitions 3 --replication-factor 2 --bootstrap-server kafka:29092
docker exec -it review-kafka-1 kafka-topics --create --topic processed-reviews --partitions 3 --replication-factor 2 --bootstrap-server kafka:29092
pause


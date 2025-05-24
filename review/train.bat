docker exec -it review-spark-master-1 /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /review/train.py
pause
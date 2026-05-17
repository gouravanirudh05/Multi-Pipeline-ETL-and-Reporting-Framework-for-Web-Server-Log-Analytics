cd /home/gourav-anirudh/Desktop/Nosql_Final_Project/Multi-Pipeline-ETL-and-Reporting-Framework-for-Web-Server-Log-Analytics

## Local machine, HDFS only, no YARN

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PIG_HOME=$HOME/pig
export HIVE_HOME=$HOME/hive
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin:$HIVE_HOME/bin:$PATH

start-dfs.sh

cd backend
source nosql_env/bin/activate
python3 server.py

## Containerized run for another machine

mkdir -p data
cp access_log_Jul95 data/
docker compose up --build

## In index.html, use:
## /app/data/access_log_Jul95

## Optional CLI checks inside the app container
docker compose exec app hdfs dfs -ls /
docker compose exec app bash -lc './pig/run.sh "[\"/app/data/access_log_Jul95\"]" records 10000 pig-docker all'
docker compose exec app bash -lc './pipelines/hive/run.sh "[\"/app/data/access_log_Jul95\"]" records 10000 hive-docker all'
docker compose exec app bash -lc './pipelines/mapreduce/run.sh "[\"/app/data/access_log_Jul95\"]" records 10000 mr-docker all'

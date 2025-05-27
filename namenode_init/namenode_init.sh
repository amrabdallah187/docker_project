

# Inside the NameNode container:
docker exec -it $(docker ps -qf "name=namenode") bash

for f in /data/new_parquet/*.parquet; do
  echo "Uploading $f → /mimic3/parquet/"
  hdfs dfs -put -f "$f" /mimic3/parquet/
done

# Check they arrived:
hdfs dfs -ls /mimic3/parquet

exit


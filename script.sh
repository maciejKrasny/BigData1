rm NYPD_Motor_Vehicle_Collisions.csv
rm zips-boroughs.csv
hadoop fs -rm -r mapreduce/output

wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/projekt1/NYPD_Motor_Vehicle_Collisions.csv
wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/projekt1/zips-boroughs.csv

hadoop fs -rm -r mapreduce
hadoop fs -mkdir -p mapreduce/input
hadoop fs -mkdir -p zipcodes

hadoop fs -copyFromLocal NYPD_Motor_Vehicle_Collisions.csv mapreduce/input/NYPD_Motor_Vehicle_Collisions.csv
hadoop fs -copyFromLocal zips-boroughs.csv zipcodes/zips-boroughs.csv

hadoop jar collisions.jar Collisions mapreduce/input mapreduce/output

hive -f hive.hql

rm -f -r result
hadoop fs -copyToLocal /mapreduce/result

cat result/*

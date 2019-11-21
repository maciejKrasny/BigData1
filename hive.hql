drop database if exists project;
create database project;
use project;

drop table if exists zips_sf;
create table zips_sf(zipcode int, district string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
load data  inpath "zipcodes/zips-boroughs.csv" into table zips_sf;

drop table if exists zips;
create table zips(zipcode int, district string) STORED AS ORC;
insert into zips select * from zips_sf;

drop table if exists accidents_sf;
create external table accidents_sf(zipcode string, street string, person string, type string, amount int) row format delimited fields terminated by "\t" stored as textfile;
load data inpath "mapreduce/output" into table accidents_sf;

drop table if exists accidents;
create external table accidents(zipcode string, street string, person string, type string, amount int) stored as orc;
insert into accidents select * from accidents_sf;

drop table if exists manhattan_accidents;
create table manhattan_accidents(street string, person string, type string, amount int) STORED AS ORC;
insert into manhattan_accidents select a.street, a.person, a.type, a.amount from accidents a join zips z on a.zipcode = z.zipcode where z.district = "MANHATTAN";

drop table if exists killed_type;
create table killed_type(street string, person string, amount int) stored as orc;
insert into killed_type select m.street, m.person, m.amount from manhattan_accidents m where m.type like "KILLED";

drop table if exists injured_type;
create table injured_type(street string, person string, amount int) stored as orc;
insert into injured_type select m.street, m.person, m.amount from manhattan_accidents m where m.type like "INJURED";

drop table if exists all_types;
create table all_types(street string, person string, killed int, injured int) stored as orc;
insert into all_types select street, person, amount, 0 from killed_type;
insert into all_types select street, person, 0, amount from injured_type;

drop table if exists final_output;
create table final_output(street string, person string, killed int, injured int) stored as ORC;
insert into final_output select street, person, sum(killed) as killed, sum(injured) as injured from all_types where person like "PEDESTRIAN" group by street, person order by killed + injured desc limit 3;
insert into final_output select street, person, sum(killed) as killed, sum(injured) as injured from all_types where person like "CYCLIST" group by street, person order by killed + injured desc limit 3;
insert into final_output select street, person, sum(killed) as killed, sum(injured) as injured from all_types where person like "MOTORIST" group by street, person order by killed + injured desc limit 3;
select * from final_output;

drop table if exists result;
create external table if not exists result (street string, person_type string, killed int, injured int) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION "/mapreduce/result";
ADD JAR /usr/lib/hive/lib/hive-hcatalog-core.jar;
insert into result select * from final_output;
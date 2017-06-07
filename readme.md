Hadoop i jego "wielkie dane"
===
[Bydgoszcz Java User Group](http://bydgoszcz.jug.pl/)

Włodzimierz Kozłowski

Kilka słów o tym czym jest *Hadoop*, pierwszy “Hello world” w *MapReduce* i do czego nam *HDFS*. Jak dostać się do danych, które “mają miliardy wierszy” a my nadal jesteśmy silnie przywiązani do *SQL*. Pojawi się *Hive*, *HBase*, *Apache Phoenix* i *Kundera*. Wszystko to nie z perspektywy eksperta, a programisty, który próbuje się w tym ogarnąć.

### MapReduce

pliki wejściowe 
```
hdfs dfs -rm -r -skipTrash /user/root
hdfs dfs -mkdir /user/root
hdfs dfs -ls /

hdfs dfs -put dane/lektury
hdfs dfs -ls lektury
```
uruchomienie zadania i wyniki
```
yarn jar mapreduce-1.0.jar pl.cafebabe.jug.hadoop.mapreduce.wordcount.WordCountRunner /user/root/lektury /user/root/wyniki-wc
hdfs dfs -ls wyniki-wc
hdfs dfs -cat wyniki-wc/part-r-00000 | sort -k2 -r -n | head -n 20
```

```
hdfs dfs -mkdir -p movielens/movies
hdfs dfs -mkdir -p movielens/ratings
hdfs dfs -put dane/movielens/ml-1m/movies.dat movielens/movies
hdfs dfs -put dane/movielens/ml-1m/ratings.dat movielens/ratings

yarn jar mapreduce-1.0.jar pl.cafebabe.jug.hadoop.mapreduce.movielens.genres.GenresRunner /user/root/movielens/movies /user/root/wyniki-genres
hdfs dfs -cat wyniki-genres/part-r-00000 | sort -k2 -r -n | head -n 20

yarn jar mapreduce-1.0.jar pl.cafebabe.jug.hadoop.mapreduce.movielens.ratings.RatingsRunner /user/root/movielens/movies /user/root/movielens/ratings /user/root/wyniki-ratings
hdfs dfs -cat wyniki-ratings/part-r-00000 | sort -t$'\t' -k4 -r -n | head -n 20
```

### Hive

konsola

```
hive
```

utworzenie bazy danych

```
-- utworzenie bazy danych jeśli nie istnieje
CREATE DATABASE IF NOT EXISTS movielens;
SHOW databases;
USE movielens;
```

utworzenie tabele i załadowanie danych

```
-- internal table
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
/*
DROP TABLE IF EXISTS movies_hi;
CREATE TABLE IF NOT EXISTS movies_hi (
  movie_id STRING,
  title STRING,
  genres STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
*/

-- https://cwiki.apache.org/confluence/display/Hive/MultiDelimitSerDe
-- Class org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe not found
-- mkdir /usr/hdp/2.6.0.3-8/hive/auxlib
-- cp /usr/hdp/2.6.0.3-8/hive/lib/hive-contrib.jar /usr/hdp/2.6.0.3-8/hive/auxlib
-- restart Hive (Ambari: http://127.0.0.1:8080)

DROP TABLE IF EXISTS movies_hi;
CREATE TABLE IF NOT EXISTS movies_hi (
  movie_id STRING,
  title STRING,
  genres STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::");

-- załadowanie danych z HDFS
-- LOAD DATA INPATH '/user/root/movielens/movies' OVERWRITE INTO TABLE movies_hi;
-- załadowanie danych z pliku lokalnego
LOAD DATA LOCAL INPATH 'dane/movielens/ml-1m/movies.dat' OVERWRITE INTO TABLE movies_hi;

-- external table
DROP TABLE IF EXISTS movies_he;
CREATE EXTERNAL TABLE IF NOT EXISTS movies_he (
  movie_id BIGINT,
  title STRING,
  genres STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
LOCATION '/user/root/movielens/movies';

DROP TABLE IF EXISTS ratings_he;
CREATE EXTERNAL TABLE IF NOT EXISTS ratings_he (
  user_id BIGINT,
  movie_id BIGINT,
  rating TINYINT,
  timestamp_ INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
LOCATION '/user/root/movielens/ratings';
```

weryfikacja
```
show tables;
select * from movies_he limit 10;
select * from ratings_he limit 10;

SELECT count(*) FROM movies_he WHERE title LIKE '%and%';
```

... i nasze problemy
```
# dla przypomnienia
hdfs dfs -cat wyniki-genres/part-r-00000 | sort -k2 -r -n | head -n 10
```
```
-- zapytanie HiveQL
SELECT genre, count(1) AS liczba
FROM movies_he
LATERAL VIEW explode(split(genres,"\\|")) latview AS genre
GROUP BY genre
ORDER BY liczba DESC
LIMIT 10;
```
```
hdfs dfs -cat wyniki-ratings/part-r-00000 | sort -t$'\t' -k4 -r -n | head -n 10
```
```
SELECT m.title, count(*) as liczba, avg(r.rating) AS ocena
FROM movies_he m, ratings_he r
WHERE m.movie_id = r.movie_id
GROUP BY m.title
HAVING liczba > 100
ORDER BY ocena desc
LIMIT 10;
```

### HBase

```
hbase shell
```
weryfikacja działania serwera
```
status
```
```
# utworzenie tabeli
-- create 'ODCZYTY', 'CF'
create 'ODCZYTY', { NAME => 'CF', VERSIONS => 3 }
list

# wstawienie danych
for i in 1..1000
    put 'ODCZYTY', "1-#{i}", 'CF:NRL', "nr #{i}"
    put 'ODCZYTY', "1-#{i}", 'CF:ZUZYCIE', rand(200)
end

for i in 1..1000
    put 'ODCZYTY', "2-#{i}", 'CF:NRL', "nr #{i}"
    put 'ODCZYTY', "2-#{i}", 'CF:ZUZYCIE', rand(200)
end

# pobranie i aktualizacja danych
get 'ODCZYTY', '1-17'
put 'ODCZYTY', '1-17', 'CF:ZUZYCIE', 99
get 'ODCZYTY', '1-17'
get 'ODCZYTY', '1-17', { COLUMN => ['CF:NRL', 'CF:ZUZYCIE'], VERSIONS => 10}
get 'ODCZYTY', '2-17'

scan 'ODCZYTY'

# https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/package-summary.html
# https://www.cloudera.com/documentation/enterprise/5-9-x/topics/admin_hbase_filtering.html
scan 'ODCZYTY', { FILTER => "ValueFilter(=, 'binary:nr 17')" }
scan 'ODCZYTY', { FILTER => "PrefixFilter('1-')" }

# usunięcie tabeli
disable 'ODCZYTY'
drop 'ODCZYTY'
```
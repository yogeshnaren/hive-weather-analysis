# Weather Data Analysis on Apache Hive

### Creating temporary table from the datafiles:
```
CREATE TABLE IF NOT EXISTS ncdc_weather_data 
( 
STATION_ID STRING, 
DATE_DATA STRING, 
ELEMENT_DATA STRING, 
VALUE_DATA INT, 
MFLAG CHAR(1), 
QFLAG CHAR(1), 
RFLAG CHAR(1), 
OBSTIME STRING 
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
location '/user/bundeyyn/PIIweather/';
```

### Creating similar Optimized Row Columnar (ORC) Table:

```
CREATE TABLE IF NOT EXISTS ncdc_weather_orc
(
ID STRING,
WDATE STRING,
ELEMENT STRING,
VALUE INT,
MFLAG CHAR(1),
QFLAG CHAR(1),
RFLAG CHAR(1),
OBSTIME STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC;
```

### Inserting all the rows from the temporary table to the ORC table:

```
INSERT OVERWRITE TABLE ncdc_weather_orc SELECT * FROM ncdc_weather_data;
```

### Deleting (dropping) the temporary table:

```
DROP TABLE ncdc_weather_data;
```

## 1.	Average TMIN, TMAX for each year excluding abnormalities or missing data

```
SELECT YEAR, avg(VALUE), ELEMENT
FROM (
SELECT *, substr(WDATE, 0, 4) AS YEAR
FROM ncdc_weather_orc
WHERE VALUE <> 9999 and VALUE <> -9999 and MFLAG <> 'P' and QFLAG = '' and RFLAG <> '' and ELEMENT in ('TMIN', 'TMAX')
) AS YEARS
GROUP BY YEAR, ELEMENT
ORDER BY YEAR, ELEMENT;
```

## 2.	Maximum TMAX, Minimum TMIN for each year excluding abnormalities or missing data

```
SELECT MIN_TMIN, MAX_TMAX, MINVAL.YEAR
FROM (
(SELECT min(VALUE) as MIN_TMIN, substr(WDATE, 0, 4) AS YEAR
FROM ncdc_weather_orc 
WHERE VALUE <> 9999 and VALUE <> -9999 and MFLAG <> 'P' and QFLAG = '' and RFLAG <> '' and ELEMENT = 'TMIN'
GROUP BY substr(WDATE, 0, 4)
ORDER BY YEAR
) AS MINVAL
INNER JOIN
(SELECT max(VALUE) as MAX_TMAX, substr(WDATE, 0, 4) AS YEAR
FROM ncdc_weather_orc 
WHERE VALUE <> 9999 and VALUE <> -9999 and MFLAG <> 'P' and QFLAG = '' and RFLAG <> '' and ELEMENT = 'TMAX'
GROUP BY substr(WDATE, 0, 4)
ORDER BY YEAR
) AS MAXVAL
ON MAXVAL.YEAR = MINVAL.YEAR)
ORDER BY MINVAL.YEAR;
```

## 3.	5 hottest , 5 coldest weather stations for each year excluding abnormalities or missing data

```
SELECT T2.HOTTEST, T1.COLDEST, T1.YEAR
FROM(
SELECT ROW_NUMBER() OVER (ORDER BY YEAR) AS SID, MINVAL.MIN_ID AS COLDEST, MINVAL.YEAR AS YEAR
FROM (
SELECT ID as MIN_ID, VALUE, substr(WDATE, 0, 4) AS YEAR,
ROW_NUMBER() OVER (PARTITION BY SUBSTR(WDATE,0,4) ORDER BY VALUE) AS RANK FROM ncdc_weather_orc
WHERE VALUE <> 9999 and VALUE <> -9999 and MFLAG <> 'P' and QFLAG = '' and RFLAG <> '' and ELEMENT = 'TMIN'
) AS MINVAL
WHERE MINVAL.RANK <= 5
) AS T1
INNER JOIN
(
SELECT ROW_NUMBER() OVER (ORDER BY YEAR) AS SID, MAXVAL.MAX_ID AS HOTTEST, MAXVAL.YEAR AS YEAR
FROM (
SELECT ID as MAX_ID, VALUE, substr(WDATE, 0, 4) AS YEAR,
ROW_NUMBER() OVER (PARTITION BY SUBSTR(WDATE,0,4) ORDER BY VALUE DESC) AS RANK FROM ncdc_weather_orc
WHERE VALUE <> 9999 and VALUE <> -9999 and MFLAG <> 'P' and QFLAG = '' and RFLAG <> '' and ELEMENT = 'TMAX'
) AS MAXVAL
WHERE MAXVAL.RANK <= 5
) AS T2
ON T2.SID = T1.SID
ORDER BY T1.YEAR;
```

## 4.	Hottest and coldest day and corresponding weather stations in the entire dataset

```
SELECT DATE_FORMAT( from_unixtime(unix_timestamp(WDATE ,'yyyyMMdd'), 'yyyy-MM-dd'), 'E') AS DAY, WDATE, ID, VALUE
FROM
(SELECT WDATE, ID, VALUE,
rank() over (ORDER BY VALUE DESC) as RANK
FROM ncdc_weather_orc
WHERE VALUE <> 9999 and VALUE <> -9999 and MFLAG <> 'P' and QFLAG = '' and RFLAG <> '' and ELEMENT = 'TMAX'
) S
WHERE S.RANK = 1
union
SELECT DATE_FORMAT( from_unixtime(unix_timestamp(WDATE ,'yyyyMMdd'), 'yyyy-MM-dd'), 'E') AS DAY, WDATE, ID, VALUE
FROM
(SELECT WDATE, ID, VALUE,
rank() over (ORDER BY VALUE) as RANK
FROM ncdc_weather_orc
WHERE VALUE <> 9999 and VALUE <> -9999 and MFLAG <> 'P' and QFLAG = '' and RFLAG <> '' and ELEMENT = 'TMIN'
) S
WHERE S.RANK = 1;
```
## 5.	Median TMIN, TMAX for each year and corresponding weather stations

```
SELECT T1.* FROM (SELECT MAX(ID) AS WEATHER_STATION, PERCENTILE (CAST (VALUE AS BIGINT), 0.5) AS MEDIAN_VALUE, SUBSTR(YEAR,1,4) AS YEAR, MAX(ELEMENT) AS TEMP FROM my_weather_dangetgr
WHERE value != 9999 and value != -9999 and MVAL <> 'P' and RVAL <> '' and QVAL = '' AND ELEMENT='TMIN'
GROUP BY SUBSTR(YEAR,1,4)
) AS T1
UNION
SELECT T2.* FROM (SELECT MAX(ID) AS WEATHER_STATION, PERCENTILE (CAST (VALUE AS BIGINT), 0.5) AS MEDIAN_VALUE, SUBSTR(YEAR,1,4) AS YEAR, MAX(ELEMENT) AS TEMP FROM my_weather_dangetgr
WHERE value != 9999 and value != -9999 and MVAL <> 'P' and RVAL <> '' and QVAL = '' AND ELEMENT='TMAX'
GROUP BY SUBSTR(YEAR,1,4)
) AS T2
ORDER BY YEAR;
```

## 6.	Median TMIN, TMAX for the entire dataset

```
SELECT T1.* FROM (SELECT PERCENTILE (CAST (VALUE AS BIGINT), 0.5) AS MEDIAN_VALUE, MAX(ELEMENT) FROM my_weather_dangetgr
WHERE value != 9999 and value != -9999 and MVAL <> 'P' and RVAL <> '' and QVAL = '' AND ELEMENT='TMIN') AS T1
UNION
SELECT T2.* FROM (SELECT PERCENTILE (CAST (VALUE AS BIGINT), 0.5) AS MEDIAN_VALUE, MAX(ELEMENT) FROM my_weather_dangetgr
WHERE value != 9999 and value != -9999 and MVAL <> 'P' and RVAL <> '' and QVAL = '' AND ELEMENT='TMAX') AS T2
```

## Method Used:

Apache Hive:  Hive is also developed by Apache as a data warehouse software project over Hadoop for data analysis, summarization and selections. It provides easy approach similar to SQL and relational database management system by allowing us to query in various databases and file systems such as HDFS etc.

## Why choose Apache Hive:

As a data science enthusiast, I have trained myself on relational databases such as SQLServer and Oracle. Hive, having huge similarities with the ANSI coded SQL language, it was easier to adapt and generate queries and perform analysis required for the analysis. The integration with the Hadoop clusters seems faster than the runs I had performed with the pySpark implementation. Also, Hive tables are directly defined on HDFS so it was easier to transfer data and perform operations over it. It saved from writing complex codes while sticking to SQL queries for data processing.

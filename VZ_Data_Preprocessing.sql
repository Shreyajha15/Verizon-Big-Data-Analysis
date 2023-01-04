
/* ******************************************************************************************************************************** 

Loading the Weather Data into HDFS
===================================
Step-1 : Download the weather.tar.gz file from the link shared by VZ officials
Step-2 : Move the weather.tar.gz file from your local file system where it was downloaded to the Hadoop cluster smvhadoop02.utdallas.edu using WinSCP.
-- Use credentials Host name : smvhadoop02.utdallas.edu , Username: cloudera, Password: cloudera to setup a connection bridge on WinSCP
Step-3 : Unzip the weather.tar.gz file using the following command to get weather.tar file.
-- gunzip weather.tar.gz
Step-4 : The file weather.tar is still a compressed file. Need to unzip this as well. Do this in 2 sub-steps:-
-- 1) Create a directory VZ_Weather_Data using the following command:-
		-- mkdir VZ_Weather_Data
-- 2) Use the following command to unzip and copy all the files in weather.tar to VZ_Weather_Data
		-- tar -C VZ_Weather_Data -xvf weather.tar
Step-5 : The uncompressed weather data is now in the folder VZ_Weather_Data which is in the local filesystem of the Hadoop Cluster. Use the following command to move this folder to the HDFS.
-- hadoop fs -put VZ_Weather_Data

## The following set of code snippets demonstrates the step by step result of loading the weather data into HDFS. ##

*/

-- Unzipping the gz file

[cloudera@quickstart Downloads]$ cd Verizon
[cloudera@quickstart Verizon]$ ls -lrt
total 57744
-rw-rw-r-- 1 cloudera cloudera 59121775 Jan 30 16:06 weather.tar.gz
[cloudera@quickstart Verizon]$ gunzip weather.tar.gz
[cloudera@quickstart Verizon]$ ls -lrt
total 505220
-rw-rw-r-- 1 cloudera cloudera 517345280 Jan 30 16:06 weather.tar

--Unzip tar file

mkdir VZ_Weather_Data
tar -C VZ_Weather_Data -xvf weather.tar
-- Extracted the contents of tar file to VZ_Weather_Data

[cloudera@quickstart Verizon]$ ls -lrt
total 505228
drwx------ 157 cloudera cloudera      4096 Jan 29 11:44 VZ_Weather_Data
-rw-rw-r--   1 cloudera cloudera 517345280 Jan 30 16:06 weather.tar
[cloudera@quickstart Verizon]$ pwd
/home/cloudera/Downloads/Verizon


--Copying VZ_Weather_Data to HDFS from Local file system

hadoop fs -put VZ_Weather_Data

--Verifying that files are properly moved to HDFS

[cloudera@quickstart Downloads]$ hadoop fs -ls VZ_Weather_Data
Found 155 items
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:13 VZ_Weather_Data/IXABH
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:14 VZ_Weather_Data/IXAGW
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:14 VZ_Weather_Data/IXAXH
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:15 VZ_Weather_Data/IXBGA
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:16 VZ_Weather_Data/IXBGH
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:17 VZ_Weather_Data/IXBGX
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:18 VZ_Weather_Data/IXBGY
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:19 VZ_Weather_Data/IXBGZ
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:20 VZ_Weather_Data/IXBHG
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:22 VZ_Weather_Data/IXCBC
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:23 VZ_Weather_Data/IXCHW
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:24 VZ_Weather_Data/IXCHY
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:26 VZ_Weather_Data/IXCWG
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 14:27 VZ_Weather_Data/IXCXX


/* ********************************************************************************************************************************

Loading the KPI Data into HDFS
==============================
Step-1 : Download the kpi.tar.gz file from the link shared by VZ officials
Step-2 : Move the kpi.tar.gz file from your local file system where it was downloaded to the Hadoop cluster smvhadoop02.utdallas.edu using WinSCP.
-- Use credentials Host name : smvhadoop02.utdallas.edu , Username: cloudera, Password: cloudera to setup a connection bridge on WinSCP
Step-3 : Unzip the kpi.tar.gz file using the following command to get weather.tar file.
-- gunzip kpi.tar.gz
Step-4 : The file kpi.tar is still a compressed file. Need to unzip this as well. Do this in 2 sub-steps:-
-- 1) Create a directory VZ_KPI_Data using the following command:-
		-- mkdir VZ_KPI_Data
-- 2) Use the following command to unzip and copy all the files in kpi.tar to VZ_KPI_Data
		-- tar -C VZ_KPI_Data -xvf kpi.tar
Step-5 : The uncompressed KPI data is now in the folder VZ_KPI_Data which is in the local filesystem of the Hadoop Cluster. Use the following command to move this folder to the HDFS.
-- hadoop fs -put VZ_KPI_Data

## The following set of code snippets demonstrates the step by step result of loading the KPI data into HDFS. ##

*/

--Unzipping the gz file

[cloudera@quickstart Downloads]$ cd Verizon
[cloudera@quickstart Verizon]$ ls -lrt
total 576756
drwx------ 157 cloudera cloudera      4096 Jan 29 11:44 VZ_Weather_Data
-rw-rw-r--   1 cloudera cloudera  59121775 Jan 30 16:06 weather.tar.gz
-rw-rw-r--   1 cloudera cloudera 531463143 Feb  6 21:56 kpi.tar.gz
[cloudera@quickstart Verizon]$ gunzip kpi.tar.gz
[cloudera@quickstart Verizon]$ ls -lrt
total 2893168
drwx------ 157 cloudera cloudera       4096 Jan 29 11:44 VZ_Weather_Data
-rw-rw-r--   1 cloudera cloudera   59121775 Jan 30 16:06 weather.tar.gz
-rw-rw-r--   1 cloudera cloudera 2903470080 Feb  6 21:56 kpi.tar
[cloudera@quickstart Verizon]$ 

--Unzip tar file

[cloudera@quickstart Verizon]$ tar -C VZ_KPI_Data -xvf kpi.tar
201712.csv
201801.csv
201802.csv
201803.csv
201804.csv
201805.csv
201806.csv
201807.csv
201808.csv
201809.csv
201810.csv
201811.csv
201812.csv
[cloudera@quickstart Verizon]$ ls -lrt
total 2893172
drwx------ 157 cloudera cloudera       4096 Jan 29 11:44 VZ_Weather_Data
-rw-rw-r--   1 cloudera cloudera   59121775 Jan 30 16:06 weather.tar.gz
-rw-rw-r--   1 cloudera cloudera 2903470080 Feb  6 21:56 kpi.tar
drwxrwxr-x   2 cloudera cloudera       4096 Feb  6 22:18 VZ_KPI_Data
[cloudera@quickstart Verizon]$ 

--Copying VZ_Weather_Data to HDFS from Local file system
hadoop fs -put VZ_KPI_Data

[cloudera@quickstart VZ_KPI_Data]$ hadoop fs -ls
Found 5 items
drwx------   - cloudera cloudera          0 2019-02-05 13:00 .Trash
drwxr-xr-x   - cloudera cloudera          0 2019-02-05 15:41 .sparkStaging
drwx------   - cloudera cloudera          0 2018-11-01 12:44 .staging
drwxr-xr-x   - cloudera cloudera          0 2019-02-06 22:25 VZ_KPI_Data
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 16:48 VZ_Weather_Data

--Verifying that files are properly moved to HDFS

[cloudera@quickstart VZ_KPI_Data]$ hadoop fs -ls VZ_KPI_Data
Found 13 items
-rw-r--r--   1 cloudera cloudera  218930188 2019-02-06 22:27 VZ_KPI_Data/201712.csv
-rw-r--r--   1 cloudera cloudera  193206563 2019-02-06 22:30 VZ_KPI_Data/201801.csv
-rw-r--r--   1 cloudera cloudera  203524178 2019-02-06 22:30 VZ_KPI_Data/201802.csv
-rw-r--r--   1 cloudera cloudera  139064311 2019-02-06 22:31 VZ_KPI_Data/201803.csv
-rw-r--r--   1 cloudera cloudera  207948515 2019-02-06 22:31 VZ_KPI_Data/201804.csv
-rw-r--r--   1 cloudera cloudera  110021335 2019-02-06 22:31 VZ_KPI_Data/201805.csv
-rw-r--r--   1 cloudera cloudera  239363164 2019-02-06 22:32 VZ_KPI_Data/201806.csv
-rw-r--r--   1 cloudera cloudera  263110379 2019-02-06 22:32 VZ_KPI_Data/201807.csv
-rw-r--r--   1 cloudera cloudera  264207740 2019-02-06 22:33 VZ_KPI_Data/201808.csv
-rw-r--r--   1 cloudera cloudera  243747195 2019-02-06 22:33 VZ_KPI_Data/201809.csv
-rw-r--r--   1 cloudera cloudera  266969072 2019-02-06 22:34 VZ_KPI_Data/201810.csv
-rw-r--r--   1 cloudera cloudera  266900159 2019-02-06 22:34 VZ_KPI_Data/201811.csv
-rw-r--r--   1 cloudera cloudera  286460912 2019-02-06 22:35 VZ_KPI_Data/201812.csv


/* ********************************************************************************************************************************

Loading the KPI Data into a Hive Table (needed as Spark 1.6 cannot be used to load .csv files directly into dataframe)
======================================
Step-1 : Open the Spark shell in the terminal using the below command. It will prompt a Scala console.
-- spark-shell
Step-2 : Create an empty Hive table vz_kpi_hourly. 
Step-3 : Load all the .csv files in VZ_KPI_Data inro vz_kpi_hourly.
Step-4 : Open another Putty terminal and after login start hive using the following command:-
--hive
Step-5 : In this new terminal create another empty Hive table vz_kpi_data with the appropriate datatype of the columns.
Step-6 : Insert data from vz_kpi_hourly to vz_kpi_data after making necessary processing in the vz_kpi_hourly.
Step-7: Drop vz_kpi_hourly

## The following set of code snippets demonstrates the step by step result of loading the KPI data into Hive table vz_kpi_data. ##

*/

--Created an empty Hive table with the required schema
sqlContext.sql("CREATE TABLE vz_kpi_hourly (trans_dt string,hr string,radio_number string,sc_ab_rel double,sc_call_ans double,sc_call_attempts double,sc_call_completes double,sc_call_drops double,sc_call_drops_incl_ho double,sc_call_setup_completes double,sc_call_setup_fails double,adjusted_sip_dc_rate double,adjusted_sip_sc_dcs double,adjusted_sip_sc_dc_perc double,handover_attempts double,handoverfailures double,handover_failure_perc double,qci1_bd_perc double,location string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TextFile")

--Loading KPI Data for all records
sqlContext.sql("LOAD DATA INPATH 'VZ_KPI_Data/*' OVERWRITE INTO TABLE vz_kpi_hourly")

--Create a table vz_kpi_data in Hive using the below schema
CREATE TABLE vz_kpi_data 
(
trans_dt string,
hr int,
radio_number string,
sc_ab_rel double,
sc_call_ans double,
sc_call_attempts double,
sc_call_completes double,
sc_call_drops double,
sc_call_drops_incl_ho double,
sc_call_setup_completes double,
sc_call_setup_fails double,
adjusted_sip_dc_rate double,
adjusted_sip_sc_dcs double,
adjusted_sip_sc_dc_perc double,
handover_attempts double,
handoverfailures double,
handover_failure_perc double,
qci1_bd_perc double,
location_id string
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' STORED AS TextFile;

--Manipulate the data in vz_kpi_hourly and move it into vz_kpi_data
INSERT OVERWRITE TABLE vz_kpi_data
SELECT from_unixtime(unix_timestamp(trans_dt ,'yyyyMMdd'), 'yyyy-MM-dd') as trans_dt,int(hr),radio_number string,sc_ab_rel,sc_call_ans,sc_call_attempts,sc_call_completes,sc_call_drops,sc_call_drops_incl_ho,
sc_call_setup_completes,sc_call_setup_fails,adjusted_sip_dc_rate,adjusted_sip_sc_dcs,adjusted_sip_sc_dc_perc,handover_attempts,handoverfailures,handover_failure_perc,qci1_bd_perc,location
FROM vz_kpi_hourly;

DROP TABLE vz_kpi_hourly;

/* ********************************************************************************************************************************

Loading the VZ_Weather_Data into a Spark Dataframe
==================================================
Challenges -
	1) JSON file contains Daily field which is not needed for our purpose.
	2) Zip code information is not available in JSON files.
	3) Date is not present in JSON files.
	4) Zip Code is part of the filename in the format ZipCode_YYYYMMDD.json
	5) Have to extract the Zip Code from the filename while loading the data from each file.

Use the following command in the code snippet to load the weather data from VZ_Weather_Data into a spark dataframe weather_data

*/

val weather_data = sqlContext.read.option("multiLine","true").option("mode","PERMISSIVE").json("weather/*/*").select("timezone","offset","latitude","longitude","hourly.*").withColumn("filename", input_file_name).withColumn("fname", split(col("filename"), "\\/").getItem(7)).withColumn("location_id", split(col("fname"), "\\_").getItem(0)).withColumn("date1", split(col("fname"), "\\_").getItem(1)).withColumn("data", explode(col("data"))).select("location_id","timezone","offset","latitude","longitude","data.*").withColumn("hr", hour(from_unixtime(col("time")))).withColumn("Date",to_date(from_unixtime(col("time")))).select("location_id","Date","hr","apparentTemperature","cloudCover","dewPoint","humidity","precipIntensity","precipProbability","precipType","pressure","temperature","uvIndex","visibility","windBearing","windGust","windSpeed").withColumn("trans_dt",col("Date").cast("String")).select("location_id","trans_dt","hr","apparentTemperature","cloudCover","dewPoint","humidity","precipIntensity","precipProbability","precipType","pressure","temperature","uvIndex","visibility","windBearing","windGust","windSpeed")

/*

--Read all the JSON files together
sqlContext.read.option("multiLine","true").option("mode","PERMISSIVE").json("VZ_Weather_Data/*/*")
--Select only the top-level columns needed. hourly is another JSON. So, hourly.* will access all the fields within the JSON.
.select("timezone","offset","latitude","longitude","hourly.*")
--Create a column to store the file name.
.withColumn("filename", input_file_name)
--Create a column to get the location_id/zip code from the file name.
.withColumn("fname", split(col("filename"), "\\/").getItem(7)).withColumn("location_id", split(col("fname"), "\\_").getItem(0))
--Column data is an array of JSONs. First need to explode it to store the data array into a new column data.
.withColumn("data", explode(col("data")))
--Select all the columns. Note now data is a JSON.
.select("location_id","timezone","offset","latitude","longitude","data.*")
--Fetch hr from the column time. time was there in the JSON data.
.withColumn("hr", hour(from_unixtime(col("time"))))
--Create a column Date to extract the date from the time column.
.withColumn("Date",to_date(from_unixtime(col("time"))))
--Create a column trans_dt to store Date casted to string.
.withColumn("trans_dt",col("Date").cast("String"))
--Select only the columns of interest.
.select("location_id","trans_dt","hr","apparentTemperature","cloudCover","dewPoint","humidity","precipIntensity","precipProbability","precipType","pressure","temperature","uvIndex","visibility","windBearing","windGust","windSpeed")


/* ********************************************************************************************************************************

Loading the KPI Data into a Spark dataframe
===========================================

val kpi_data=sqlContext.sql("SELECT * FROM vz_kpi_data")
val kpi_data_new=kpi_data.select("location_id","trans_dt","hr","radio_number","sc_ab_rel","sc_call_ans","sc_call_attempts","sc_call_completes","sc_call_drops","sc_call_drops_incl_ho","sc_call_setup_completes","sc_call_setup_fails","adjusted_sip_dc_rate","adjusted_sip_sc_dcs","adjusted_sip_sc_dc_perc","handover_attempts","handoverfailures","handover_failure_perc","qci1_bd_perc")

/* ********************************************************************************************************************************

Join the weather and KPI data
==============================

--Create a dataframe merged_data to join the weather data and the kpi data on location_id, trans_dt and hr.
val merged_data=weather_data.join(kpi_data_new,Seq("location_id","trans_dt","hr"),"inner")

/* ********************************************************************************************************************************

Save the merged data into a Hive table for ready to load any time.
==================================================================

--Saving in parquet format to a Hive table vz_merged_data.
-- Hive table is automatically created.
--If Hive table pre-existed, it is overwritten.
merged_data.write.mode("overwrite").format("parquet").saveAsTable("vz_merged_data")


/* ********************************************************************************************************************************

Export the Hive table to a CSV file which can be used for analysis in Tableau
==============================================================================

--sed removes \t and white space and replaces that with ,
hive -e 'set hive.cli.print.header=true; select * from vz_merged_data' | sed 's/[\t]/,/g'  > /home/cloudera/Desktop/data/merged_vz.csv



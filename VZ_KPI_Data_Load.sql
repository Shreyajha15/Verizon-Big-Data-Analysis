/* Unzipping the gz file */

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



/* Unzip tar file */

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


/* Copying VZ_Weather_Data to HDFS from Local file system */

[cloudera@quickstart VZ_KPI_Data]$ hadoop fs -ls
Found 5 items
drwx------   - cloudera cloudera          0 2019-02-05 13:00 .Trash
drwxr-xr-x   - cloudera cloudera          0 2019-02-05 15:41 .sparkStaging
drwx------   - cloudera cloudera          0 2018-11-01 12:44 .staging
drwxr-xr-x   - cloudera cloudera          0 2019-02-06 22:25 VZ_KPI_Data
drwxr-xr-x   - cloudera cloudera          0 2019-02-01 16:48 VZ_Weather_Data

hadoop fs -put kpi

/* Verifying that files are properly moved to HDFS */

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

################################################################################################3

/* Checking for a sample CSV file */

VZ_KPI_Data/201712.csv

[cloudera@quickstart VZ_KPI_Data]$ head -8 201805.csv
trans_dt,hr,radio_number,sc_ab_rel,sc_call_ans,sc_call_attempts,sc_call_completes,sc_call_drops,sc_call_drops_incl_ho,sc_call_setup_completes,sc_call_setup_fails,adjusted_sip_dc_rate,adjusted_sip_sc_dcs,adjusted_sip_sc_dc%,handover_attempts,handoverfailures,handover_failure%,qci1_bd%,location
20180530,02,405_1_1,0,0,0,0,0,0,0,0,0,0,0.0,9,0,0.0,0.0,IYHHI
20180530,02,405_1_2,0,0,0,0,0,0,0,0,0,0,0.0,1,0,0.0,0.0,IYHHI
20180530,02,405_1_3,0,0,0,0,0,0,0,0,0,0,0.0,1,0,0.0,0.0,IYHHI
20180530,02,405_2_1,0,0,0,0,0,0,0,0,0,0,0.0,3,0,0.0,0.0,IYHHI
20180530,02,405_2_2,0,0,0,0,0,0,0,0,0,0,0.0,7,0,0.0,0.0,IYHHI
20180530,02,405_2_3,0,0,0,0,0,0,0,0,0,0,0.0,3,0,0.0,0.0,IYHHI
20180530,02,405_3_1,0,0,0,0,0,0,0,0,0,0,0.0,3,0,0.0,0.0,IYHHI

/* Created an empty HIve table with the required schema */
sqlContext.sql("CREATE TABLE vz_kpi_hourly (trans_dt string,hr string,radio_number string,sc_ab_rel double,sc_call_ans double,sc_call_attempts double,sc_call_completes double,sc_call_drops double,sc_call_drops_incl_ho double,sc_call_setup_completes double,sc_call_setup_fails double,adjusted_sip_dc_rate double,adjusted_sip_sc_dcs double,adjusted_sip_sc_dc_perc double,handover_attempts double,handoverfailures double,handover_failure_perc double,qci1_bd_perc double,location string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TextFile")

/*
sqlContext.sql("LOAD DATA INPATH 'VZ_KPI_Data/201801.csv' OVERWRITE INTO TABLE vz_kpi_hourly")
val df_kpi=sqlContext.sql("SELECT * FROM vz_kpi_hourly")
*/

sqlContext.sql("CREATE TABLE student (studentID int,studentName string)")

df_kpi.show(30,false)



/* LOading KPI Data for all records */

sqlContext.sql("LOAD DATA INPATH 'kpi/*' OVERWRITE INTO TABLE vz_kpi_hourly")
val df_kpi=sqlContext.sql("SELECT * FROM vz_kpi_hourly")
    
df_kpi.persist()

scala> df_kpi.count()
res3: Long = 41028107


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

INSERT OVERWRITE TABLE vz_kpi_data
SELECT from_unixtime(unix_timestamp(trans_dt ,'yyyyMMdd'), 'yyyy-MM-dd') as trans_dt,int(hr),radio_number string,sc_ab_rel,sc_call_ans,sc_call_attempts,sc_call_completes,sc_call_drops,sc_call_drops_incl_ho,
sc_call_setup_completes,sc_call_setup_fails,adjusted_sip_dc_rate,adjusted_sip_sc_dcs,adjusted_sip_sc_dc_perc,handover_attempts,handoverfailures,handover_failure_perc,qci1_bd_perc,location
FROM vz_kpi_hourly;


val kpi_data=sqlContext.sql("SELECT * FROM vz_kpi_data")
kpi_data.persist()

kpi_data.show(50,false)

scala> kpi_data.show(50,false)
+----------+---+------------+---------+-----------+----------------+-----------------+-------------+---------------------+-----------------------+-------------------+--------------------+-------------------+-----------------------+-----------------+----------------+---------------------+------------+-----------+
|trans_dt  |hr |radio_number|sc_ab_rel|sc_call_ans|sc_call_attempts|sc_call_completes|sc_call_drops|sc_call_drops_incl_ho|sc_call_setup_completes|sc_call_setup_fails|adjusted_sip_dc_rate|adjusted_sip_sc_dcs|adjusted_sip_sc_dc_perc|handover_attempts|handoverfailures|handover_failure_perc|qci1_bd_perc|location_id|
+----------+---+------------+---------+-----------+----------------+-----------------+-------------+---------------------+-----------------------+-------------------+--------------------+-------------------+-----------------------+-----------------+----------------+---------------------+------------+-----------+
|2018-09-27|7  |879_3_4     |4.0      |20.0       |24.0            |20.0             |0.0          |0.0                  |20.0                   |0.0                |0.0                 |0.0                |0.0                    |91.0             |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|9  |879_1_1     |33.0     |105.0      |138.0           |105.0            |0.0          |0.0                  |105.0                  |0.0                |0.0                 |0.0                |0.0                    |759.0            |4.0             |0.53                 |0.96        |IYGXC      |
|2018-09-27|9  |879_1_2     |65.0     |126.0      |191.0           |126.0            |0.0          |0.0                  |126.0                  |0.0                |0.0                 |0.0                |0.0                    |641.0            |2.0             |0.31                 |0.0         |IYGXC      |
|2018-09-27|9  |879_1_3     |39.0     |113.0      |152.0           |112.0            |1.0          |1.0                  |113.0                  |0.0                |0.0                 |0.0                |0.0                    |619.0            |2.0             |0.32                 |0.0         |IYGXC      |
|2018-09-27|9  |879_1_4     |56.0     |144.0      |200.0           |144.0            |0.0          |0.0                  |144.0                  |0.0                |0.0                 |0.0                |0.0                    |195.0            |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|9  |879_2_1     |35.0     |156.0      |191.0           |156.0            |0.0          |0.0                  |156.0                  |0.0                |0.0                 |0.0                |0.0                    |599.0            |2.0             |0.33                 |0.63        |IYGXC      |
|2018-09-27|9  |879_2_2     |40.0     |84.0       |124.0           |84.0             |0.0          |0.0                  |84.0                   |0.0                |0.0                 |0.0                |0.0                    |480.0            |3.0             |0.62                 |0.0         |IYGXC      |
|2018-09-27|9  |879_2_3     |39.0     |95.0       |134.0           |94.0             |1.0          |1.0                  |95.0                   |2.0                |0.0                 |1.0                |1.05                   |124.0            |12.0            |9.68                 |0.0         |IYGXC      |
|2018-09-27|9  |879_2_4     |55.0     |147.0      |202.0           |146.0            |0.0          |0.0                  |147.0                  |0.0                |0.0                 |0.0                |0.0                    |44.0             |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|9  |879_3_1     |21.0     |78.0       |99.0            |78.0             |0.0          |0.0                  |78.0                   |0.0                |0.0                 |0.0                |0.0                    |489.0            |4.0             |0.82                 |0.0         |IYGXC      |
|2018-09-27|9  |879_3_2     |21.0     |74.0       |95.0            |74.0             |0.0          |0.0                  |74.0                   |0.0                |0.0                 |0.0                |0.0                    |328.0            |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|9  |879_3_3     |21.0     |48.0       |69.0            |48.0             |0.0          |0.0                  |48.0                   |0.0                |0.0                 |0.0                |0.0                    |111.0            |11.0            |9.91                 |0.0         |IYGXC      |
|2018-09-27|9  |879_3_4     |20.0     |65.0       |85.0            |65.0             |0.0          |0.0                  |65.0                   |0.0                |0.0                 |0.0                |0.0                    |103.0            |1.0             |0.97                 |0.0         |IYGXC      |
|2018-09-27|11 |879_1_1     |46.0     |138.0      |184.0           |138.0            |1.0          |1.0                  |138.0                  |0.0                |0.0                 |1.0                |0.72                   |743.0            |1.0             |0.13                 |0.71        |IYGXC      |
|2018-09-27|11 |879_1_2     |48.0     |148.0      |196.0           |148.0            |0.0          |0.0                  |148.0                  |0.0                |0.0                 |0.0                |0.0                    |708.0            |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|11 |879_1_3     |58.0     |133.0      |191.0           |133.0            |0.0          |0.0                  |133.0                  |0.0                |0.0                 |0.0                |0.0                    |646.0            |2.0             |0.31                 |0.0         |IYGXC      |
|2018-09-27|11 |879_1_4     |92.0     |187.0      |279.0           |187.0            |0.0          |0.0                  |187.0                  |0.0                |0.0                 |0.0                |0.0                    |137.0            |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|11 |879_2_1     |52.0     |168.0      |220.0           |166.0            |2.0          |2.0                  |168.0                  |0.0                |0.0                 |1.0                |0.6                    |545.0            |5.0             |0.92                 |0.0         |IYGXC      |
|2018-09-27|11 |879_2_2     |40.0     |100.0      |140.0           |100.0            |0.0          |0.0                  |100.0                  |0.0                |0.0                 |0.0                |0.0                    |452.0            |4.0             |0.88                 |0.0         |IYGXC      |
|2018-09-27|11 |879_2_3     |30.0     |91.0       |121.0           |90.0             |1.0          |1.0                  |91.0                   |0.0                |0.0                 |0.0                |0.0                    |96.0             |7.0             |7.29                 |0.0         |IYGXC      |
|2018-09-27|11 |879_2_4     |79.0     |145.0      |224.0           |145.0            |0.0          |0.0                  |144.0                  |1.0                |0.0                 |0.0                |0.0                    |55.0             |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|11 |879_3_1     |21.0     |65.0       |86.0            |65.0             |0.0          |0.0                  |65.0                   |0.0                |0.0                 |0.0                |0.0                    |550.0            |6.0             |1.09                 |0.0         |IYGXC      |
|2018-09-27|11 |879_3_2     |33.0     |42.0       |75.0            |42.0             |0.0          |0.0                  |42.0                   |0.0                |0.0                 |0.0                |0.0                    |337.0            |2.0             |0.59                 |0.0         |IYGXC      |
|2018-09-27|11 |879_3_3     |9.0      |57.0       |66.0            |57.0             |0.0          |0.0                  |57.0                   |0.0                |0.0                 |0.0                |0.0                    |110.0            |2.0             |1.82                 |0.0         |IYGXC      |
|2018-09-27|11 |879_3_4     |23.0     |57.0       |80.0            |57.0             |0.0          |0.0                  |57.0                   |0.0                |0.0                 |0.0                |0.0                    |108.0            |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|13 |879_1_1     |29.0     |96.0       |125.0           |96.0             |0.0          |0.0                  |96.0                   |0.0                |0.0                 |0.0                |0.0                    |768.0            |5.0             |0.65                 |0.93        |IYGXC      |
|2018-09-27|13 |879_1_2     |50.0     |126.0      |176.0           |126.0            |0.0          |0.0                  |126.0                  |0.0                |0.0                 |0.0                |0.0                    |649.0            |1.0             |0.15                 |0.0         |IYGXC      |
|2018-09-27|13 |879_1_3     |56.0     |123.0      |179.0           |122.0            |1.0          |1.0                  |123.0                  |0.0                |0.0                 |1.0                |0.81                   |660.0            |2.0             |0.3                  |0.0         |IYGXC      |
|2018-09-27|13 |879_1_4     |79.0     |191.0      |270.0           |190.0            |1.0          |1.0                  |191.0                  |0.0                |0.0                 |0.0                |0.0                    |186.0            |1.0             |0.54                 |0.0         |IYGXC      |
|2018-09-27|13 |879_2_1     |50.0     |155.0      |205.0           |155.0            |1.0          |1.0                  |155.0                  |0.0                |0.0                 |1.0                |0.65                   |587.0            |1.0             |0.17                 |1.2         |IYGXC      |
|2018-09-27|13 |879_2_2     |40.0     |97.0       |137.0           |95.0             |0.0          |0.0                  |97.0                   |0.0                |0.0                 |0.0                |0.0                    |465.0            |1.0             |0.22                 |0.0         |IYGXC      |
|2018-09-27|13 |879_2_3     |26.0     |76.0       |102.0           |76.0             |0.0          |0.0                  |76.0                   |0.0                |0.0                 |0.0                |0.0                    |103.0            |8.0             |7.77                 |0.0         |IYGXC      |
|2018-09-27|13 |879_2_4     |77.0     |144.0      |221.0           |144.0            |0.0          |0.0                  |144.0                  |0.0                |0.0                 |0.0                |0.0                    |37.0             |1.0             |2.7                  |0.66        |IYGXC      |
|2018-09-27|13 |879_3_1     |21.0     |89.0       |110.0           |88.0             |1.0          |1.0                  |89.0                   |0.0                |0.0                 |0.0                |0.0                    |547.0            |4.0             |0.73                 |0.0         |IYGXC      |
|2018-09-27|13 |879_3_2     |22.0     |63.0       |85.0            |63.0             |0.0          |0.0                  |63.0                   |0.0                |0.0                 |0.0                |0.0                    |347.0            |5.0             |1.44                 |0.0         |IYGXC      |
|2018-09-27|13 |879_3_3     |20.0     |44.0       |64.0            |44.0             |0.0          |0.0                  |44.0                   |0.0                |0.0                 |0.0                |0.0                    |148.0            |6.0             |4.05                 |2.27        |IYGXC      |
|2018-09-27|13 |879_3_4     |16.0     |44.0       |60.0            |44.0             |0.0          |0.0                  |44.0                   |0.0                |0.0                 |0.0                |0.0                    |107.0            |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|13 |879_0_0     |0.0      |1.0        |1.0             |1.0              |0.0          |0.0                  |1.0                    |0.0                |0.0                 |0.0                |0.0                    |0.0              |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|14 |879_1_1     |43.0     |136.0      |179.0           |134.0            |2.0          |2.0                  |136.0                  |0.0                |0.0                 |2.0                |1.47                   |967.0            |6.0             |0.62                 |0.0         |IYGXC      |
|2018-09-27|14 |879_1_2     |68.0     |193.0      |261.0           |192.0            |1.0          |1.0                  |193.0                  |0.0                |0.0                 |0.0                |0.0                    |769.0            |3.0             |0.39                 |0.0         |IYGXC      |
|2018-09-27|14 |879_1_3     |58.0     |166.0      |224.0           |165.0            |1.0          |1.0                  |166.0                  |0.0                |0.0                 |0.0                |0.0                    |784.0            |3.0             |0.38                 |0.0         |IYGXC      |
|2018-09-27|14 |879_1_4     |73.0     |213.0      |286.0           |213.0            |0.0          |0.0                  |213.0                  |0.0                |0.0                 |0.0                |0.0                    |175.0            |0.0             |0.0                  |0.0         |IYGXC      |
|2018-09-27|14 |879_2_1     |49.0     |166.0      |215.0           |164.0            |1.0          |1.0                  |166.0                  |0.0                |0.0                 |0.0                |0.0                    |727.0            |5.0             |0.69                 |0.0         |IYGXC      |
|2018-09-27|14 |879_2_2     |54.0     |88.0       |142.0           |87.0             |0.0          |0.0                  |88.0                   |0.0                |0.0                 |0.0                |0.0                    |490.0            |3.0             |0.61                 |0.0         |IYGXC      |
|2018-09-27|14 |879_2_3     |22.0     |84.0       |106.0           |84.0             |0.0          |0.0                  |84.0                   |0.0                |0.0                 |0.0                |0.0                    |159.0            |7.0             |4.4                  |0.0         |IYGXC      |
|2018-09-27|14 |879_2_4     |80.0     |153.0      |233.0           |151.0            |0.0          |0.0                  |153.0                  |0.0                |0.0                 |0.0                |0.0                    |40.0             |1.0             |2.5                  |0.0         |IYGXC      |
|2018-09-27|14 |879_3_1     |33.0     |101.0      |134.0           |100.0            |1.0          |1.0                  |101.0                  |0.0                |0.0                 |0.0                |0.0                    |668.0            |4.0             |0.6                  |0.0         |IYGXC      |
|2018-09-27|14 |879_3_2     |29.0     |49.0       |78.0            |49.0             |0.0          |0.0                  |49.0                   |0.0                |0.0                 |0.0                |0.0                    |350.0            |9.0             |2.57                 |0.0         |IYGXC      |
|2018-09-27|14 |879_3_3     |20.0     |48.0       |68.0            |48.0             |0.0          |0.0                  |48.0                   |0.0                |0.0                 |0.0                |0.0                    |131.0            |16.0            |12.21                |0.0         |IYGXC      |
|2018-09-27|14 |879_3_4     |23.0     |53.0       |76.0            |53.0             |0.0          |0.0                  |53.0                   |0.0                |0.0                 |0.0                |0.0                    |116.0            |1.0             |0.86                 |0.0         |IYGXC      |
+----------+---+------------+---------+-----------+----------------+-----------------+-------------+---------------------+-----------------------+-------------------+--------------------+-------------------+-----------------------+-----------------+----------------+---------------------+------------+-----------+
only showing top 50 rows

drop table vz_kpi_hourly;

res4: Long = 41028107



/* Unzipping the gz file */

[cloudera@quickstart Downloads]$ cd Verizon
[cloudera@quickstart Verizon]$ ls -lrt
total 57744
-rw-rw-r-- 1 cloudera cloudera 59121775 Jan 30 16:06 weather.tar.gz
[cloudera@quickstart Verizon]$ gunzip weather.tar.gz
[cloudera@quickstart Verizon]$ ls -lrt
total 505220
-rw-rw-r-- 1 cloudera cloudera 517345280 Jan 30 16:06 weather.tar

/* Unzip tar file */

mkdir VZ_Weather_Data
tar -C VZ_Weather_Data -xvf weather.tar
-- Extracted the contents of tar file to VZ_Weather_Data

[cloudera@quickstart Verizon]$ ls -lrt
total 505228
drwx------ 157 cloudera cloudera      4096 Jan 29 11:44 VZ_Weather_Data
-rw-rw-r--   1 cloudera cloudera 517345280 Jan 30 16:06 weather.tar
[cloudera@quickstart Verizon]$ pwd
/home/cloudera/Downloads/Verizon


/* Copying VZ_Weather_Data to HDFS from Local file system */

hadoop fs -put VZ_Weather_Data

/* Verifying that files are properly moved to HDFS */

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


################################################################################################3

/* Checking with a sample JSON file */

VZ_Weather_Data/IZXIG/IZXIG_20181201.json

[cloudera@quickstart Downloads]$ hadoop fs -cat VZ_Weather_Data/IZXIG/IZXIG_20181201.json
{"latitude":34.72,"longitude":-80.77,"timezone":"America/New_York","hourly":{"summary":"Rain starting in the afternoon, continuing until evening.","icon":"rain","data":[{"time":1543640400,"summary":"Mostly Cloudy","icon":"partly-cloudy-night","precipIntensity":0,"precipProbability":0,"temperature":54.67,"apparentTemperature":54.67,"dewPoint":47.03,"humidity":0.75,"pressure":1019.23,"windSpeed":0.1,"windGust":0.62,"windBearing":180,"cloudCover":0.75,"uvIndex":0,"visibility":10},{"time":1543644000,"summary":"Partly Cloudy","icon":"partly-cloudy-night","precipIntensity":0,"precipProbability":0,"temperature":52.32,"apparentTemperature":52.32,"dewPoint":49.18,"humidity":0.89,"pressure":1018.89,"windSpeed":0.27,"windGust":1.43,"windBearing":207,"cloudCover":0.44,"uvIndex":0,"visibility":10},{"time":1543647600,"summary":"Mostly Cloudy","icon":"partly-cloudy-night","precipIntensity":0,"precipProbability":0,"temperature":53.09,"apparentTemperature":53.09,"dewPoint":45.93,"humidity":0.77,"pressure":1018.52,"windSpeed":3.72,"windGust":3.86,"windBearing":201,"cloudCover":0.86,"uvIndex":0,"visibility":10},{"time":1543651200,"summary":"Overcast","icon":"cloudy","precipIntensity":0,"precipProbability":0,"temperature":53.45,"apparentTemperature":53.45,"dewPoint":44.25,"humidity":0.71,"pressure":1018.62,"windSpeed":3.25,"windGust":3.55,"windBearing":193,"cloudCover":1,"uvIndex":0,"visibility":10},{"time":1543654800,"summary":"Overcast","icon":"cloudy","precipIntensity":0,"precipProbability":0,"temperature":51.94,"apparentTemperature":51.94,"dewPoint":45.43,"humidity":0.78,"pressure":1018.24,"windSpeed":1.13,"windGust":1.27,"windBearing":200,"cloudCover":1,"uvIndex":0,"visibility":10},{"time":1543658400,"summary":"Overcast","icon":"cloudy","precipIntensity":0,"precipProbability":0,"temperature":52.41,"apparentTemperature":52.41,"dewPoint":44.59,"humidity":0.75,"pressure":1017.68,"windSpeed":2.83,"windGust":3.08,"windBearing":187,"cloudCover":1,"uvIndex":0,"visibility":10},{"time":1543662000,"summary":"Overcast","icon":"cloudy","precipIntensity":0,"precipProbability":0,"temperature":50.41,"apparentTemperature":50.41,"dewPoint":44.4,"humidity":0.8,"pressure":1017.68,"windSpeed":0.1,"windGust":0.82,"windBearing":225,"cloudCover":1,"uvIndex":0,"visibility":10},{"time":1543665600,"summary":"Overcast","icon":"cloudy","precipIntensity":0,"precipProbability":0,"temperature":49.84,"apparentTemperature":49.84,"dewPoint":44.14,"humidity":0.81,"pressure":1017.94,"windSpeed":0,"windGust":0.82,"cloudCover":1,"uvIndex":0,"visibility":10},{"time":1543669200,"summary":"Partly Cloudy","icon":"partly-cloudy-day","precipIntensity":0,"precipProbability":0,"temperature":49.5,"apparentTemperature":49.5,"dewPoint":44.31,"humidity":0.82,"pressure":1018.28,"windSpeed":0,"windGust":0.82,"cloudCover":0.57,"uvIndex":0,"visibility":10},{"time":1543672800,"summary":"Clear","icon":"clear-day","precipIntensity":0,"precipProbability":0,"temperature":52.21,"apparentTemperature":52.21,"dewPoint":45.83,"humidity":0.79,"pressure":1018.19,"windSpeed":0,"windGust":0.62,"cloudCover":0,"uvIndex":1,"visibility":10},{"time":1543676400,"summary":"Mostly Cloudy","icon":"partly-cloudy-day","precipIntensity":0,"precipProbability":0,"temperature":53.95,"apparentTemperature":53.95,"dewPoint":45.84,"humidity":0.74,"pressure":1018.54,"windSpeed":0,"windGust":0.82,"cloudCover":0.75,"uvIndex":1,"visibility":10},{"time":1543680000,"summary":"Mostly Cloudy","icon":"partly-cloudy-day","precipIntensity":0.0007,"precipProbability":0.1,"precipType":"rain","temperature":54.33,"apparentTemperature":54.33,"dewPoint":48.35,"humidity":0.8,"pressure":1018.78,"windSpeed":1.01,"windGust":1.01,"windBearing":170,"cloudCover":0.9,"uvIndex":2,"visibility":6},{"time":1543683600,"summary":"Overcast","icon":"cloudy","precipIntensity":0.0129,"precipProbability":0.48,"precipType":"rain","temperature":53.55,"apparentTemperature":53.55,"dewPoint":53.03,"humidity":0.98,"pressure":1018.26,"windSpeed":2.67,"windGust":3.12,"windBearing":95,"cloudCover":1,"uvIndex":2,"visibility":4.57},{"time":1543687200,"summary":"Light Rain","icon":"rain","precipIntensity":0.041,"precipProbability":0.75,"precipType":"rain","temperature":53.44,"apparentTemperature":53.44,"dewPoint":52.33,"humidity":0.96,"pressure":1016.79,"windSpeed":1.68,"windGust":1.77,"windBearing":96,"cloudCover":1,"uvIndex":2,"visibility":3},{"time":1543690800,"summary":"Light Rain","icon":"rain","precipIntensity":0.0337,"precipProbability":0.64,"precipType":"rain","temperature":53.73,"apparentTemperature":53.73,"dewPoint":53.23,"humidity":0.98,"pressure":1015.64,"windSpeed":3.54,"windGust":3.58,"windBearing":90,"cloudCover":1,"uvIndex":2,"visibility":2.19},{"time":1543694400,"summary":"Foggy","icon":"fog","precipIntensity":0.0191,"precipProbability":0.61,"precipType":"rain","temperature":54.17,"apparentTemperature":54.17,"dewPoint":53.46,"humidity":0.97,"pressure":1014.01,"windSpeed":3.06,"windGust":3.37,"windBearing":86,"cloudCover":1,"uvIndex":1,"visibility":1.86},{"time":1543698000,"summary":"Light Rain","icon":"rain","precipIntensity":0.048,"precipProbability":0.82,"precipType":"rain","temperature":54.94,"apparentTemperature":54.94,"dewPoint":54.38,"humidity":0.98,"pressure":1013.38,"windSpeed":4.76,"windGust":5.08,"windBearing":101,"cloudCover":1,"uvIndex":0,"visibility":1.8},{"time":1543701600,"summary":"Rain","icon":"rain","precipIntensity":0.147,"precipProbability":0.94,"precipType":"rain","temperature":55.21,"apparentTemperature":55.21,"dewPoint":55.08,"humidity":1,"pressure":1012.94,"windSpeed":6.63,"windGust":7.11,"windBearing":101,"cloudCover":1,"uvIndex":0,"visibility":2.86},{"time":1543705200,"summary":"Rain","icon":"rain","precipIntensity":0.2032,"precipProbability":1,"precipType":"rain","temperature":55.54,"apparentTemperature":55.54,"dewPoint":55.36,"humidity":0.99,"pressure":1012.41,"windSpeed":6.13,"windGust":8.08,"windBearing":92,"cloudCover":1,"uvIndex":0,"visibility":2.43},{"time":1543708800,"summary":"Rain","icon":"rain","precipIntensity":0.1749,"precipProbability":0.89,"precipType":"rain","temperature":55.88,"apparentTemperature":55.88,"dewPoint":55.4,"humidity":0.98,"pressure":1012.13,"windSpeed":4.39,"windGust":4.9,"windBearing":96,"cloudCover":1,"uvIndex":0,"visibility":3.19},{"time":1543712400,"summary":"Light Rain","icon":"rain","precipIntensity":0.0455,"precipProbability":0.95,"precipType":"rain","temperature":56.89,"apparentTemperature":56.89,"dewPoint":55.73,"humidity":0.96,"pressure":1012,"windSpeed":5.38,"windGust":5.38,"windBearing":104,"cloudCover":1,"uvIndex":0,"visibility":2.78},{"time":1543716000,"summary":"Overcast","icon":"cloudy","precipIntensity":0.0155,"precipProbability":0.6,"precipType":"rain","temperature":57.76,"apparentTemperature":57.76,"dewPoint":56.55,"humidity":0.96,"pressure":1011.11,"windSpeed":3.57,"windGust":3.57,"windBearing":122,"cloudCover":1,"uvIndex":0,"visibility":5.29},{"time":1543719600,"summary":"Overcast","icon":"cloudy","precipIntensity":0.0098,"precipProbability":0.58,"precipType":"rain","temperature":57.96,"apparentTemperature":58.06,"dewPoint":57.39,"humidity":0.98,"pressure":1010.54,"windSpeed":2.91,"windGust":3.01,"windBearing":138,"cloudCover":1,"uvIndex":0,"visibility":3.57},{"time":1543723200,"summary":"Overcast","icon":"cloudy","precipIntensity":0.0053,"precipProbability":0.43,"precipType":"rain","temperature":59.59,"apparentTemperature":59.7,"dewPoint":58.03,"humidity":0.95,"pressure":1009.83,"windSpeed":2.44,"windGust":2.44,"windBearing":162,"cloudCover":1,"uvIndex":0,"visibility":3.14}]},"daily":{"data":[{"time":1543640400,"summary":"Rain starting in the afternoon, continuing until evening.","icon":"rain","sunriseTime":1543666425,"sunsetTime":1543702420,"moonPhase":0.81,"precipIntensity":0.0315,"precipIntensityMax":0.2032,"precipIntensityMaxTime":1543705200,"precipProbability":1,"precipType":"rain","temperatureHigh":55.88,"temperatureHighTime":1543708800,"temperatureLow":56.89,"temperatureLowTime":1543712400,"apparentTemperatureHigh":55.88,"apparentTemperatureHighTime":1543708800,"apparentTemperatureLow":56.89,"apparentTemperatureLowTime":1543712400,"dewPoint":50.39,"humidity":0.88,"pressure":1015.82,"windSpeed":1.94,"windGust":8.08,"windGustTime":1543705200,"windBearing":119,"cloudCover":0.89,"uvIndex":2,"uvIndexTime":1543680000,"visibility":6.36,"temperatureMin":49.5,"temperatureMinTime":1543669200,"temperatureMax":59.59,"temperatureMaxTime":1543723200,"apparentTemperatureMin":49.5,"apparentTemperatureMinTime":1543669200,"apparentTemperatureMax":59.7,"apparentTemperatureMaxTime":1543723200}]},"offset":-5}[cloudera@quickstart Downloads]$ 

/* Validated using JSON Validator */


/* Challenges -
	1) JSON file contains Daily field which is not needed for our purpose.
	2) Zip code information is not available in JSON files.
	3) Date is not present in JSON files.
	4) Both Zip Code and Date are part of the filename in the format ZipCode_YYYYMMDD.json
	5) Have to extract the Zip Code and the Date from the filename while loading the data from each file.
*/

spark-shell


/*

val df = sqlContext.read.option("multiLine","true").option("mode","PERMISSIVE").json("VZ_Weather_Data/IZXIG/IZXIG_20181201.json").select("timezone","offset","latitude","longitude","hourly.*").withColumn("data", explode(col("data"))).select("timezone","offset","latitude","longitude","data.*")


val fname =sqlContext.read.text("VZ_Weather_Data/IZXIG/IZXIG_20181201.json").withColumn("filename", input_file_name).select("filename").withColumn("fname", split(col("filename"), "\\/").getItem(7)).withColumn("zip", split(col("fname"), "\\_").getItem(0)).withColumn("date1", split(col("fname"), "\\_").getItem(1)).withColumn("date", split(col("date1"), "\\.").getItem(0)).select("zip","date")

scala> fname.show(false)
+-----+--------+
|zip  |date    |
+-----+--------+
|IZXIG|20181201|
+-----+--------+

*/


val df_data = sqlContext.read.option("multiLine","true").option("mode","PERMISSIVE").json("weather/IZXIG/IZXIG_20181201.json").select("timezone","offset","latitude","longitude","hourly.*").withColumn("filename", input_file_name).withColumn("fname", split(col("filename"), "\\/").getItem(7)).withColumn("zip", split(col("fname"), "\\_").getItem(0)).withColumn("data", explode(col("data"))).select("zip","date","timezone","offset","latitude","longitude","data.*").withColumn("hr", hour(from_unixtime(col("time")))).withColumn("Date",to_date(from_unixtime(col("time")))).select("zip","Date","hr","offset","latitude","longitude","apparentTemperature","cloudCover","dewPoint","humidity","icon","precipIntensity","precipProbability","precipType","pressure","summary","temperature","uvIndex","visibility","windBearing","windGust","windSpeed")

scala> df_data.show(30,false)
+-----+----------+---+------+--------+---------+-------------------+----------+--------+--------+-------------------+---------------+-----------------+----------+--------+-------------+-----------+-------+----------+-----------+--------+---------+
|zip  |Date      |hr |offset|latitude|longitude|apparentTemperature|cloudCover|dewPoint|humidity|icon               |precipIntensity|precipProbability|precipType|pressure|summary      |temperature|uvIndex|visibility|windBearing|windGust|windSpeed|
+-----+----------+---+------+--------+---------+-------------------+----------+--------+--------+-------------------+---------------+-----------------+----------+--------+-------------+-----------+-------+----------+-----------+--------+---------+
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |54.67              |0.75      |47.03   |0.75    |partly-cloudy-night|0.0            |0.0              |null      |1019.23 |Mostly Cloudy|54.67      |0      |10.0      |180        |0.62    |0.1      |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |52.32              |0.44      |49.18   |0.89    |partly-cloudy-night|0.0            |0.0              |null      |1018.89 |Partly Cloudy|52.32      |0      |10.0      |207        |1.43    |0.27     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |53.09              |0.86      |45.93   |0.77    |partly-cloudy-night|0.0            |0.0              |null      |1018.52 |Mostly Cloudy|53.09      |0      |10.0      |201        |3.86    |3.72     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |53.45              |1.0       |44.25   |0.71    |cloudy             |0.0            |0.0              |null      |1018.62 |Overcast     |53.45      |0      |10.0      |193        |3.55    |3.25     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |51.94              |1.0       |45.43   |0.78    |cloudy             |0.0            |0.0              |null      |1018.24 |Overcast     |51.94      |0      |10.0      |200        |1.27    |1.13     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |52.41              |1.0       |44.59   |0.75    |cloudy             |0.0            |0.0              |null      |1017.68 |Overcast     |52.41      |0      |10.0      |187        |3.08    |2.83     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |50.41              |1.0       |44.4    |0.8     |cloudy             |0.0            |0.0              |null      |1017.68 |Overcast     |50.41      |0      |10.0      |225        |0.82    |0.1      |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |49.84              |1.0       |44.14   |0.81    |cloudy             |0.0            |0.0              |null      |1017.94 |Overcast     |49.84      |0      |10.0      |null       |0.82    |0.0      |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |49.5               |0.57      |44.31   |0.82    |partly-cloudy-day  |0.0            |0.0              |null      |1018.28 |Partly Cloudy|49.5       |0      |10.0      |null       |0.82    |0.0      |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |52.21              |0.0       |45.83   |0.79    |clear-day          |0.0            |0.0              |null      |1018.19 |Clear        |52.21      |1      |10.0      |null       |0.62    |0.0      |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |53.95              |0.75      |45.84   |0.74    |partly-cloudy-day  |0.0            |0.0              |null      |1018.54 |Mostly Cloudy|53.95      |1      |10.0      |null       |0.82    |0.0      |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |54.33              |0.9       |48.35   |0.8     |partly-cloudy-day  |7.0E-4         |0.1              |rain      |1018.78 |Mostly Cloudy|54.33      |2      |6.0       |170        |1.01    |1.01     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |53.55              |1.0       |53.03   |0.98    |cloudy             |0.0129         |0.48             |rain      |1018.26 |Overcast     |53.55      |2      |4.57      |95         |3.12    |2.67     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |53.44              |1.0       |52.33   |0.96    |rain               |0.041          |0.75             |rain      |1016.79 |Light Rain   |53.44      |2      |3.0       |96         |1.77    |1.68     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |53.73              |1.0       |53.23   |0.98    |rain               |0.0337         |0.64             |rain      |1015.64 |Light Rain   |53.73      |2      |2.19      |90         |3.58    |3.54     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |54.17              |1.0       |53.46   |0.97    |fog                |0.0191         |0.61             |rain      |1014.01 |Foggy        |54.17      |1      |1.86      |86         |3.37    |3.06     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |54.94              |1.0       |54.38   |0.98    |rain               |0.048          |0.82             |rain      |1013.38 |Light Rain   |54.94      |0      |1.8       |101        |5.08    |4.76     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |55.21              |1.0       |55.08   |1.0     |rain               |0.147          |0.94             |rain      |1012.94 |Rain         |55.21      |0      |2.86      |101        |7.11    |6.63     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |55.54              |1.0       |55.36   |0.99    |rain               |0.2032         |1.0              |rain      |1012.41 |Rain         |55.54      |0      |2.43      |92         |8.08    |6.13     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |55.88              |1.0       |55.4    |0.98    |rain               |0.1749         |0.89             |rain      |1012.13 |Rain         |55.88      |0      |3.19      |96         |4.9     |4.39     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |56.89              |1.0       |55.73   |0.96    |rain               |0.0455         |0.95             |rain      |1012.0  |Light Rain   |56.89      |0      |2.78      |104        |5.38    |5.38     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |57.76              |1.0       |56.55   |0.96    |cloudy             |0.0155         |0.6              |rain      |1011.11 |Overcast     |57.76      |0      |5.29      |122        |3.57    |3.57     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |58.06              |1.0       |57.39   |0.98    |cloudy             |0.0098         |0.58             |rain      |1010.54 |Overcast     |57.96      |0      |3.57      |138        |3.01    |2.91     |
|IZXIG|2018-12-01|12 |-5    |34.72   |-80.77   |59.7               |1.0       |58.03   |0.95    |cloudy             |0.0053         |0.43             |rain      |1009.83 |Overcast     |59.59      |0      |3.14      |162        |2.44    |2.44     |
+-----+----------+---+------+--------+---------+-------------------+----------+--------+--------+-------------------+---------------+-----------------+----------+--------+-------------+-----------+-------+----------+-----------+--------+---------+





df_data.printSchema()

scala> df_data.printSchema()
root
 |-- zip: string (nullable = true)
 |-- Date: date (nullable = true)
 |-- hr: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- apparentTemperature: double (nullable = true)
 |-- cloudCover: double (nullable = true)
 |-- dewPoint: double (nullable = true)
 |-- humidity: double (nullable = true)
 |-- icon: string (nullable = true)
 |-- precipIntensity: double (nullable = true)
 |-- precipProbability: double (nullable = true)
 |-- precipType: string (nullable = true)
 |-- pressure: double (nullable = true)
 |-- summary: string (nullable = true)
 |-- temperature: double (nullable = true)
 |-- uvIndex: long (nullable = true)
 |-- visibility: double (nullable = true)
 |-- windBearing: long (nullable = true)
 |-- windGust: double (nullable = true)
 |-- windSpeed: double (nullable = true)




/* Read all JSON files into a dataframe */

val weather_data = sqlContext.read.option("multiLine","true").option("mode","PERMISSIVE").json("weather/*/*").select("timezone","offset","latitude","longitude","hourly.*").withColumn("filename", input_file_name).withColumn("fname", split(col("filename"), "\\/").getItem(7)).withColumn("location_id", split(col("fname"), "\\_").getItem(0)).withColumn("date1", split(col("fname"), "\\_").getItem(1)).withColumn("data", explode(col("data"))).select("location_id","timezone","offset","latitude","longitude","data.*").withColumn("hr", hour(from_unixtime(col("time")))).withColumn("trans_dt",to_date(from_unixtime(col("time")))).select("location_id","trans_dt","hr","offset","latitude","longitude","apparentTemperature","cloudCover","dewPoint","humidity","icon","precipIntensity","precipProbability","precipType","pressure","summary","temperature","uvIndex","visibility","windBearing","windGust","windSpeed")
df_data.persist()


df_data.count()
res1: Long = 1357940  (we should have 1361520 records)

df_data.groupBy("zip").count().show(200,false)

+-----+-----+                                                                   
|zip  |count|
+-----+-----+
|IYGXY|8757 |
|IZXGX|8757 |
|IYGXZ|8757 |
|IZXGY|8757 |
|IXXGH|8784 |
|IYIHA|8709 |
|IYIHB|8757 |
|IYIHC|8757 |
|IXIGZ|8784 |
|IYGYA|8757 |
|IYIHG|8757 |
|IYIHH|8757 |
|IYHGA|8757 |
|IYIHI|8757 |
|IZXHC|8757 |
|IYHGB|8757 |
|IYHGC|8757 |
|IYCGI|8757 |
|IYGYG|8757 |
|IYGYH|8757 |
|IZXHG|8757 |
|IXHGC|8784 |
|IZXBA|8757 |
|IYHAA|8757 |
|IYIHW|8757 |
|IZXBC|8757 |
|IYHAB|8757 |
|IYIHX|8757 |
|IXAXH|8784 |
|IXHGH|8784 |
|IYHAC|8757 |
|IYIHZ|8757 |
|IXXHC|8760 |
|IYGYW|8757 |
|IZXBI|8757 |
|IXIBI|8784 |
|IYHGX|8757 |
|IYIIA|8757 |
|IXWGA|8784 |
|IYHGY|8757 |
|IYAXZ|8757 |
|IYIIH|8757 |
|IXWGH|8784 |
|IYHHB|8755 |
|IYHHC|8757 |
|IYBGA|8757 |
|IYGZG|8757 |
|IYGZH|8757 |
|IZXIG|8757 |
|IYGZI|8757 |
|IYAYG|8757 |
|IYHAX|8753 |
|IYHHG|8757 |
|IYHAY|8753 |
|IYHHH|8757 |
|IYHHI|8757 |
|IXBGA|8784 |
|IYIIW|8757 |
|IYGGH|8760 |
|IYIIX|8757 |
|IXWGZ|8760 |
|IXBGH|8784 |
|IXCBC|8784 |
|IYGZX|8757 |
|IYGZY|8757 |
|IYGAB|8757 |
|IYHHX|8757 |
|IYHHZ|8757 |
|IXCHW|8784 |
|IXIIZ|8784 |
|IXCHY|8784 |
|IYICW|8757 |
|IYGAH|8784 |
|IYGGW|8784 |
|IYHIB|8757 |
|IYIWI|8757 |
|IXBGX|8784 |
|IXBGY|8784 |
|IXBGZ|8784 |
|IYHIG|8757 |
|IZCIW|8757 |
|IYCCA|8757 |
|IYAGA|8757 |
|IYGAW|8757 |
|IYGAX|8757 |
|IYGHI|8784 |
|IYIWW|8757 |
|IXBHG|8784 |
|IYHCG|8757 |
|IYIWZ|8757 |
|IYAZW|8757 |
|IYHCI|8757 |
|IYHIX|8753 |
|IYHIY|8753 |
|IYHIZ|8757 |
|IYIXA|8757 |
|IYIXB|8757 |
|IYGHW|8784 |
|IYCCX|8757 |
|IYIXG|8757 |
|IYHWA|8757 |
|IYHWB|8757 |
|IYWCG|8757 |
|IZGIG|8757 |
|IYCWI|8757 |
|IYGIC|8784 |
|IXAGW|8784 |
|IXCWG|8784 |
|IYGIG|8784 |
|IYGIH|8784 |
|IYAAY|8757 |
|IYIXX|8757 |
|IYIXY|8757 |
|IYGCB|8757 |
|IYABA|8757 |
|IYABC|8757 |
|IYHWY|8757 |
|IZBWB|8757 |
|IYGCI|8757 |
|IYGIX|8784 |
|IYIYG|8757 |
|IYHXA|8757 |
|IYIYI|8757 |
|IYHXB|8757 |
|IZGCY|8757 |
|IYHXG|8753 |
|IXABH|8784 |
|IYGCW|8757 |
|IYABX|8757 |
|IZWXI|8757 |
|IYWXA|8757 |
|IYACA|8757 |
|IZGWX|8753 |
|IXCXX|8784 |
|IYACH|8757 |
|IYACI|8757 |
|IZIGI|8757 |
|IYIGA|8733 |
|IYIGB|8710 |
|IYIGC|8757 |
|IYGXA|8757 |
|IYGXC|8757 |
|IYIGH|8733 |
|IZXGB|8757 |
|IYAWA|8757 |
|IYIGI|8709 |
|IYAWB|8757 |
|IYWYI|8757 |
|IYIGW|8757 |
|IYIGX|8757 |
|IYIGY|8757 |
|IYIGZ|8733 |
|IXXGC|8784 |
|IZXAG|8757 |
|IZXAI|8757 |
+-----+-----+

val df_chck=df_data.groupBy("zip","date").agg(countDistinct("time").alias("distinct_hrs"))

df_chck.where("distinct_hrs<24").show(5000,false)

+-----+-----------+
|  zip|count(date)|
+-----+-----------+
|IYGXY|        366|
|IZXGX|        366|
|IYGXZ|        366|
|IZXGY|        366|
|IXXGH|        366|
|IYIHA|        364|
|IYIHB|        366|
|IYIHC|        366|
|IXIGZ|        366|
|IYGYA|        366|
|IYIHG|        366|
|IYIHH|        366|
|IYHGA|        366|
|IYIHI|        366|
|IZXHC|        366|
|IYHGB|        366|
|IYHGC|        366|
|IYCGI|        366|
|IYGYG|        366|
|IYGYH|        366|
+-----+-----------+
only showing top 20 rows


scala> df_chck.where("distinct_hrs<24").show(5000,false)

df_data.registerTempTable("weather")
val results = sqlContext.sql("SELECT * FROM weather")

df_data.write.format("orc").saveAsTable("vz_weather")

df_data.coalesce(1).write.option("header", "true").save("weather.csv")








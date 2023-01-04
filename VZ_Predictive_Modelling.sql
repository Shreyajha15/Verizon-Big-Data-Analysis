/* Reading merged data from the Hive table vz_merged_data*/
val merged_data= sqlContext.sql("SELECT * FROM vz_merged_data")

/* Dropping NULL values from the data */
val merged_data_out=merged_data.na.drop()

/* Dropping values from the data where absolute value of Handover Failure % is greater than 100% */
val merged_data=merged_data_out.where(col("handover_failure_perc")<=math.abs(100))

/* Dropping values from the data where absolute value of adjusted_sip_sc_dc_perc is greater than 100 */
val merged_data_out=merged_data.where(col("adjusted_sip_sc_dc_perc")<=math.abs(100))

/* Creating column to store actual dropped calls % (Call Drops/Call Attempts)*/
val merged_data_fin=merged_data_out.withColumn("actual_dc_perc",col("sc_call_drops")/col("sc_call_attempts"))
val merged_data_out=merged_data_fin

/* String Indexing the Radio Number (Similar to Label Encoding)*/
import org.apache.spark.ml.feature.StringIndexer
val indexer = new StringIndexer().setInputCol("radio_number").setOutputCol("radio_number_enc").fit(merged_data_out)
val merged_data_new = indexer.transform(merged_data_out)

/* Creating a column make_dispatch_decision Y(1) or N(0) depending on whether absolute value of SIP DC % is greater than 1% or not. */
val merged_data_fin=merged_data_new.withColumn("make_dispatch_decision",when(col("adjusted_sip_sc_dc_perc")<=math.abs(1),1).otherwise(0))
-- This value of 1% can be adjusted.

/* Caching the final dataframe for faster procesing */
merged_data_fin.persist()

/* Schema of the final dataframe */
merged_data_fin.printSchema()

root
 |-- location_id: string (nullable = true)
 |-- trans_dt: string (nullable = true)
 |-- hr: integer (nullable = true)
 |-- apparentTemperature: double (nullable = true)
 |-- cloudCover: double (nullable = true)
 |-- dewPoint: double (nullable = true)
 |-- humidity: double (nullable = true)
 |-- precipIntensity: double (nullable = true)
 |-- precipProbability: double (nullable = true)
 |-- precipType: string (nullable = true)
 |-- pressure: double (nullable = true)
 |-- temperature: double (nullable = true)
 |-- uvIndex: long (nullable = true)
 |-- visibility: double (nullable = true)
 |-- windBearing: long (nullable = true)
 |-- windGust: double (nullable = true)
 |-- windSpeed: double (nullable = true)
 |-- radio_number: string (nullable = true)
 |-- sc_ab_rel: double (nullable = true)
 |-- sc_call_ans: double (nullable = true)
 |-- sc_call_attempts: double (nullable = true)
 |-- sc_call_completes: double (nullable = true)
 |-- sc_call_drops: double (nullable = true)
 |-- sc_call_drops_incl_ho: double (nullable = true)
 |-- sc_call_setup_completes: double (nullable = true)
 |-- sc_call_setup_fails: double (nullable = true)
 |-- adjusted_sip_dc_rate: double (nullable = true)
 |-- adjusted_sip_sc_dcs: double (nullable = true)
 |-- adjusted_sip_sc_dc_perc: double (nullable = true)
 |-- handover_attempts: double (nullable = true)
 |-- handoverfailures: double (nullable = true)
 |-- handover_failure_perc: double (nullable = true)
 |-- qci1_bd_perc: double (nullable = true)
 |-- actual_dc_perc: double (nullable = true)
 |-- radio_number_enc: double (nullable = true)
 |-- make_dispatch_decision: integer (nullable = false)

/* Creating a dataframe to store the mapping of actual radio number and the encoded radio number. */
-- This can be used in the future to predict values against the actual radio number as the model uses the encoded radio number. 
-- So, there has to be a way to retrieve the actual radio number and this method ensures that it is not lost.
val radio_num_df=merged_data_fin.select("radio_number","radio_number_enc").distinct
 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Remember the .csv file of the merged_data that was exported from the Hive table.
-- Follow the Jupyter Notebook to understand how initial analysis is done to understand the correlation among the various features and come up with the good models on sample data.
--The same understanding is applied to quickly build Spark 	Models on the entire dataset.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Prediction of Actual Dropped Calls % */
=========================================

/* Creating new dataframe from merged_data to have only the needed input features and output label send_dispatch */
val data_actual_dc_perc=merged_data_fin.select("radio_number_enc","hr","cloudCover","precipIntensity","temperature","uvIndex","visibility","windBearing","windSpeed","pressure","humidity","actual_dc_perc")

--Schema
data_actual_dc_perc.printSchema()
root
 |-- radio_number_enc: double (nullable = true)
 |-- hr: integer (nullable = true)
 |-- cloudCover: double (nullable = true)
 |-- precipIntensity: double (nullable = true)
 |-- temperature: double (nullable = true)
 |-- uvIndex: long (nullable = true)
 |-- visibility: double (nullable = true)
 |-- windBearing: long (nullable = true)
 |-- windSpeed: double (nullable = true)
 |-- pressure: double (nullable = true)
 |-- humidity: double (nullable = true)
 |-- actual_dc_perc: double (nullable = true)

--Sample 20 records
data_actual_dc_perc.show(false)
+----------------+---+----------+---------------+-----------+-------+----------+-----------+---------+--------+--------+--------------------+
|radio_number_enc|hr |cloudCover|precipIntensity|temperature|uvIndex|visibility|windBearing|windSpeed|pressure|humidity|actual_dc_perc      |
+----------------+---+----------+---------------+-----------+-------+----------+-----------+---------+--------+--------+--------------------+
|1089.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |0.0                 |
|1087.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |0.0                 |
|1091.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |0.007142857142857143|
|1090.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |0.0                 |
|1088.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |0.08771929824561403 |
|1086.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |0.0                 |
|1089.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |0.0                 |
|1087.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |0.0                 |
|1091.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |0.027210884353741496|
|1090.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |0.0                 |
|1088.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |0.03636363636363636 |
|1086.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |0.0                 |
|1089.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |0.0                 |
|1087.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |0.0                 |
|1091.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |0.014705882352941176|
|1090.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |0.0                 |
|1088.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |0.04                |
|1086.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |0.0                 |
|1089.0          |10 |1.0       |0.0242         |70.95      |5      |10.0      |51         |5.56     |1024.89 |0.94    |0.0                 |
|1087.0          |10 |1.0       |0.0242         |70.95      |5      |10.0      |51         |5.56     |1024.89 |0.94    |0.0                 |
+----------------+---+----------+---------------+-----------+-------+----------+-----------+---------+--------+--------+--------------------+


  
/* Creating LabeledPointRDD for applying models. */
--Mapping each row to entities of corresponding datatype to create a vector values.
--Feature Vector is a vector having all the columns excpet the last one - the dependent variable.
--Label is a vector which contains the last column in the data - the dependent variable.
--The LabeledPointRDD dc_perc_rdd is formed by mapping the dataframe data_actual_dc_perc to the schema (label,featureVector) after the above transformation.
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
val dc_perc_rdd=data_actual_dc_perc.rdd.map(row => {
    val values = Array(row.getAs[Double](0),row.getAs[Int](1),row.getAs[Double](2),row.getAs[Double](3),row.getAs[Double](4),row.getAs[Long](5),row.getAs[Double](6),row.getAs[Long](7),row.getAs[Double](8),row.getAs[Double](9),row.getAs[Double](10),row.getAs[Double](11))
    val featureVector = Vectors.dense(values.init)
    val label = values.last
	LabeledPoint(label,featureVector)
    })

--Check sample values of the RDD
dc_perc_rdd.take(10)

/* Splitting into train and test data */
val Array(trainData, testData) = dc_perc_rdd.randomSplit(Array(0.7, 0.3))


/*Using Random Forest */
--model_dc_perc is a Random Forest Model that trained on the training set using the specified configurations of the model.
--Number of trees, Impurity, Max Depth and Max number of bins.
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 10
val featureSubsetStrategy = "auto" 
val impurity = "variance"
val maxDepth = 7
val maxBins = 32
val model_dc_perc = RandomForest.trainRegressor(trainData,categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)


-- labelsAndPredictions is a RDD that is formed by applying the model on the test data.
--label is the actual value of the dependent variable in the test data.
--prediction is the predicted value of the dependent variable obtained by applying the model on the testdata.
val labelsAndPredictions = testData.map { point =>
  val prediction = model_dc_perc.predict(point.features)
  (point.label, prediction)
}

--Sample records in labelsAndPredictions
labelsAndPredictions.take(10)


--Combined set of all the features,the label and the predicted value.
val featuresLabelsAndPredictions = testData.map { point =>
  val prediction = model_dc_perc.predict(point.features)
  (point.features,point.label, prediction)
}

--Sample records in featuresLabelsAndPredictions
featuresLabelsAndPredictions.take(10)

--Converting featuresLabelsAndPredictions to a dataframe.
import sqlContext.implicits._
val df_test_dc_perc=featuresLabelsAndPredictions.toDF()
--Dataframe df_test_dc_perc has a single column. The below mechanism is used to split it into multiple columns.

--An UDF is created to split a vector into individual fields.
import org.apache.spark.sql.functions.udf
def columnExtractor(idx: Int) = udf((v: Vector) => v(idx))

--Applying the UDF on df_test_dc_perc. Now,df_test_dc_perc_pred contains the predicted data splitted correctly into columns.
val df_test_dc_perc_pred=df_test_dc_perc.withColumn("radio_number_enc",columnExtractor(0)($"_1")).withColumn("hr",columnExtractor(1)($"_1")).withColumn("cloudCover",columnExtractor(2)($"_1")).withColumn("precipIntensity",columnExtractor(3)($"_1")).withColumn("temperature",columnExtractor(4)($"_1")).withColumn("uvIndex",columnExtractor(5)($"_1")).withColumn("visibility",columnExtractor(6)($"_1")).withColumn("windBearing",columnExtractor(7)($"_1")).withColumn("windSpeed",columnExtractor(8)($"_1")).withColumn("pressure",columnExtractor(9)($"_1")).withColumn("humidity",columnExtractor(10)($"_1")).withColumn("actual_dc_perc",$"_2").withColumn("predicted_dc_perc",$"_3").select("radio_number_enc","hr","cloudCover","precipIntensity","temperature","uvIndex","visibility","windBearing","windSpeed","pressure","humidity","actual_dc_perc","predicted_dc_perc")

-- Joining with the radionumber dataframe previously created to get the actual value of the radio number.
val df_test_dc_perc_pred_fin=df_test_dc_perc_pred.join(radio_num_df,Seq("radio_number_enc"),"inner")

--Saving the entire actual and predicted test  dataset into a Hive table for future analysis.
df_test_dc_perc_pred_fin.write.mode("overwrite").format("parquet").saveAsTable("vz_test_data_pred")

--Exporting to CSV file for analysis in Tableau.
hive -e 'set hive.cli.print.header=true; select * from vz_test_data_pred' | sed 's/[\t]/,/g'  > /home/cloudera/Desktop/data/test_data_dc_pred.csv


/* Computing metrics */
import org.apache.spark.mllib.evaluation.RegressionMetrics
val metrics_dc_perc = new RegressionMetrics(labelsAndPredictions)
--Squared error
println(s"MSE = ${metrics_dc_perc.meanSquaredError}")
MSE = 0.001205837761297037

println(s"RMSE = ${metrics_dc_perc.rootMeanSquaredError}")
RMSE = 0.03472517474825774

--Mean absolute error
println(s"MAE = ${metrics_dc_perc.meanAbsoluteError}")
MAE = 0.009504880059396592

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/* Prediction of make_dispatch_decision */
=================================

/* Creating new dataframe from merged_data to have only the needed input features and output label send_dispatch */
val data_dispatch=merged_data_fin.select("radio_number_enc","hr","cloudCover","precipIntensity","temperature","uvIndex","visibility","windBearing","windSpeed","pressure","humidity","make_dispatch_decision")

data_dispatch.show(false)

scala> data_dispatch.show(false)
+----------------+---+----------+---------------+-----------+-------+----------+-----------+---------+--------+--------+----------------------+
|radio_number_enc|hr |cloudCover|precipIntensity|temperature|uvIndex|visibility|windBearing|windSpeed|pressure|humidity|make_dispatch_decision|
+----------------+---+----------+---------------+-----------+-------+----------+-----------+---------+--------+--------+----------------------+
|1089.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |1                     |
|1087.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |1                     |
|1091.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |1                     |
|1090.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |1                     |
|1088.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |0                     |
|1086.0          |14 |1.0       |0.0033         |50.92      |0      |10.0      |158        |2.76     |1020.01 |0.83    |1                     |
|1089.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |1                     |
|1087.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |1                     |
|1091.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |0                     |
|1090.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |1                     |
|1088.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |0                     |
|1086.0          |11 |1.0       |0.0383         |55.34      |4      |7.37      |7          |0.27     |1009.4  |0.96    |1                     |
|1089.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |1                     |
|1087.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |1                     |
|1091.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |1                     |
|1090.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |1                     |
|1088.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |0                     |
|1086.0          |13 |1.0       |0.0036         |72.28      |4      |6.3       |73         |6.51     |1015.99 |0.94    |1                     |
|1089.0          |10 |1.0       |0.0242         |70.95      |5      |10.0      |51         |5.56     |1024.89 |0.94    |1                     |
|1087.0          |10 |1.0       |0.0242         |70.95      |5      |10.0      |51         |5.56     |1024.89 |0.94    |1                     |
+----------------+---+----------+---------------+-----------+-------+----------+-----------+---------+--------+--------+----------------------+
only showing top 20 rows


data_dispatch.printSchema()

scala> data_dispatch.printSchema()
root
 |-- radio_number_enc: double (nullable = true)
 |-- hr: integer (nullable = true)
 |-- cloudCover: double (nullable = true)
 |-- precipIntensity: double (nullable = true)
 |-- temperature: double (nullable = true)
 |-- uvIndex: long (nullable = true)
 |-- visibility: double (nullable = true)
 |-- windBearing: long (nullable = true)
 |-- windSpeed: double (nullable = true)
 |-- pressure: double (nullable = true)
 |-- humidity: double (nullable = true)
 |-- make_dispatch_decision: integer (nullable = false)


/* Creating LabeledPointRDD for applying models. */
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
val data_dispatch_rdd=data_dispatch.rdd.map(row => {
    val values = Array(row.getAs[Double](0),row.getAs[Int](1),row.getAs[Double](2),row.getAs[Double](3),row.getAs[Double](4),row.getAs[Long](5),row.getAs[Double](6),row.getAs[Long](7),row.getAs[Double](8),row.getAs[Double](9),row.getAs[Double](10),row.getAs[Int](11))
    val featureVector = Vectors.dense(values.init)
    val label = values.last
	LabeledPoint(label,featureVector)
    })

data_dispatch_rdd.take(10)

/* Splitting into train and test data */
val Array(trainData, testData) = data_dispatch_rdd.randomSplit(Array(0.7, 0.3))


/*Using Random Forest */
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 10
val featureSubsetStrategy = "auto" 
val impurity = "gini"
val maxDepth = 10
val maxBins = 32
val model_rf_clf = RandomForest.trainClassifier(trainData,numClasses,categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

val labelAndPreds = testData.map { point =>
  val prediction = model_rf_clf.predict(point.features)
  (point.label, prediction)
}

val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
scala> val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
testErr: Double = 0.09971736833753397

println("Learned classification forest model:\n" + model_rf_clf.toDebugString)

/* Metrics */
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

--Instantiate metrics object
val metrics_dispatch = new BinaryClassificationMetrics(labelAndPreds)

--Precision
val precision = metrics_dispatch.precisionByThreshold
println(precision)
precision: Double = 0.900282631662466
--Recall
val recall = metrics_dispatch.recall 
recall: Double = 0.900282631662466
--F1-Score
val f1Score = metrics_dispatch.fMeasure
f1Score: Double = 0.900282631662466


--val importances = model_rf_clf.stage(2).asInstanceOf[RandomForestModel].featureImportances

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/* Prediction of adjusted SIP SC DC % */
=========================================

/* Creating new dataframe from merged_data to have only the needed input features and output label send_dispatch */
val data_adjusted_dc_perc=merged_data_fin.select("radio_number_enc","hr","cloudCover","precipIntensity","temperature","uvIndex","visibility","windBearing","windSpeed","pressure","humidity","adjusted_sip_sc_dc_perc")
data_adjusted_dc_perc.printSchema()

scala> data_adjusted_dc_perc.printSchema()
root
 |-- radio_number_enc: double (nullable = true)
 |-- hr: integer (nullable = true)
 |-- cloudCover: double (nullable = true)
 |-- precipIntensity: double (nullable = true)
 |-- temperature: double (nullable = true)
 |-- uvIndex: long (nullable = true)
 |-- visibility: double (nullable = true)
 |-- windBearing: long (nullable = true)
 |-- windSpeed: double (nullable = true)
 |-- pressure: double (nullable = true)
 |-- humidity: double (nullable = true)
 |-- adjusted_sip_sc_dc_perc: double (nullable = true)


data_adjusted_dc_perc.show(false)
  
/* Creating LabeledPointRDD for applying models. */
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
val adjusted_dc_perc_rdd=data_adjusted_dc_perc.rdd.map(row => {
    val values = Array(row.getAs[Double](0),row.getAs[Int](1),row.getAs[Double](2),row.getAs[Double](3),row.getAs[Double](4),row.getAs[Long](5),row.getAs[Double](6),row.getAs[Long](7),row.getAs[Double](8),row.getAs[Double](9),row.getAs[Double](10),row.getAs[Double](11))
    val featureVector = Vectors.dense(values.init)
    val label = values.last
	LabeledPoint(label,featureVector)
    })

adjusted_dc_perc_rdd.take(10)

/* Splitting into train and test data */
val Array(trainData, testData) = adjusted_dc_perc_rdd.randomSplit(Array(0.7, 0.3))


/*Using Random Forest */
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 10
val featureSubsetStrategy = "auto" 
val impurity = "variance"
val maxDepth = 7
val maxBins = 32
val model_adjusted_dc_perc = RandomForest.trainRegressor(trainData,categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

val labelsAndPredictions = testData.map { point =>
  val prediction = model_adjusted_dc_perc.predict(point.features)
  (point.label, prediction)
}

/* Computing metrics */
import org.apache.spark.mllib.evaluation.RegressionMetrics
val metrics_adjusted_dc_perc = new RegressionMetrics(labelsAndPredictions)
--Squared error
println(s"MSE = ${metrics_adjusted_dc_perc.meanSquaredError}")
MSE = 14.95915070294769

println(s"RMSE = ${metrics_adjusted_dc_perc.rootMeanSquaredError}")
RMSE = 3.867706129341743

--Mean absolute error
println(s"MAE = ${metrics_adjusted_dc_perc.meanAbsoluteError}")
MAE = 0.9305065478349819


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Prediction of Handover Failure % */
=========================================

/* Creating new dataframe from merged_data to have only the needed input features and output label send_dispatch */
val data_handover_failure_perc=merged_data_fin.select("radio_number_enc","hr","cloudCover","precipIntensity","temperature","uvIndex","visibility","windBearing","windSpeed","pressure","humidity","handover_failure_perc")
data_dispatch.printSchema()


scala> data_handover_failure_perc.printSchema()
root
 |-- radio_number_enc: double (nullable = true)
 |-- hr: integer (nullable = true)
 |-- cloudCover: double (nullable = true)
 |-- dewPoint: double (nullable = true)
 |-- precipIntensity: double (nullable = true)
 |-- precipProbability: double (nullable = true)
 |-- temperature: double (nullable = true)
 |-- uvIndex: long (nullable = true)
 |-- visibility: double (nullable = true)
 |-- windBearing: long (nullable = true)
 |-- windGust: double (nullable = true)
 |-- windSpeed: double (nullable = true)
 |-- pressure: double (nullable = true)
 |-- humidity: double (nullable = true)
 |-- send_dispatch: integer (nullable = false)

data_handover_failure_perc.show(false)
  
/* Creating LabeledPointRDD for applying models. */
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
val hf_perc_rdd=data_handover_failure_perc.rdd.map(row => {
    val values = Array(row.getAs[Double](0),row.getAs[Int](1),row.getAs[Double](2),row.getAs[Double](3),row.getAs[Double](4),row.getAs[Double](5),row.getAs[Double](6),row.getAs[Long](7),row.getAs[Double](8),row.getAs[Long](9),row.getAs[Double](10),row.getAs[Double](11),row.getAs[Double](12),row.getAs[Double](13),row.getAs[Int](14))
    val featureVector = Vectors.dense(values.init)
    val label = values.last
	LabeledPoint(label,featureVector)
    })

hf_perc_rdd.take(10)

/* Splitting into train and test data */
val Array(trainData, testData) = hf_perc_rdd.randomSplit(Array(0.7, 0.3))


/*Using Random Forest */
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 10
val featureSubsetStrategy = "auto" 
val impurity = "variance"
val maxDepth = 10
val maxBins = 32
val model_rf_clf = RandomForest.trainRegressor(trainData,categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

val labelAndPreds = testData.map { point =>
  val prediction = model_rf_clf.predict(point.features)
  (point.label, prediction)
}

val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
scala> val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
testErr: Double = 0.05478347124670578

println("Learned classification forest model:\n" + model_rf_clf.toDebugString)

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

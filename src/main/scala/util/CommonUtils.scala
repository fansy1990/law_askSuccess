package util

import org.apache.spark.sql.SparkSession

/**
  * //@Author: fansy 
  * //@Time: 2018/9/20 11:54
  * //@Email: fansy1990@foxmail.com
  */
object CommonUtils {

  val userid = "userid"
  val features = "features"
  val label = "label"
  val visit_count = "visit_count"
  val visit_duration = "visit_duration"
  val visit_relevant_count="visit_relevant_count"

  val scaled_features = "scaled_features"
  val assembled_features = "assembled_features"
  val filtered_features = "filtered_features"

  val predict_column = "predict_column"

  val spark = SparkSession
    .builder()
    .appName("Spark ")
      .master("local[2]")
    .getOrCreate()

  def getSparkSession() = spark

}

package util

import org.apache.spark.sql.SparkSession

/**
  * //@Author: fansy 
  * //@Time: 2018/9/20 11:54
  * //@Email: fansy1990@foxmail.com
  */
object CommonUtils {
  val spark = SparkSession
    .builder()
    .appName("Spark ")
      .master("local[2]")
    .getOrCreate()

  def getSparkSession() = spark

}

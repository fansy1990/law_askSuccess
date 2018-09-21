package a_01_preprocess

import java.util.Properties

import org.apache.spark.sql.DataFrame
import util.CommonUtils

/**
  * //@Author: fansy 
  * //@Time: 2018/9/20 11:50
  * //@Email: fansy1990@foxmail.com
  */
object ReadDB {

 val url = "jdbc:mysql://localhost:3306/law_fansy?useUnicode=true&characterEncoding=utf8"
  val user = "fansy"
  val password = "fansy"
  val table = "law_fansy.gz_lawyer_data"
  val driver ="com.mysql.jdbc.Driver"

  def getData(): DataFrame = {
    val  properties = new Properties()
    properties.put("user",user)
    properties.put("password",password)
    properties.put("driver",driver)
    CommonUtils.getSparkSession().read.jdbc(url,table,properties)
  }

  def main(args: Array[String]): Unit = {
    val data = getData()
    data.show(2)
  }

}

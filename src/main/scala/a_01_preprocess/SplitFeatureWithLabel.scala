package a_01_preprocess

import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml._
import _root_.util.CommonUtils._
import a_01_preprocess.ReadDB._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SQLContext, SaveMode}


/**
  * //@Author: fansy 
  * //@Time: 2018/9/21 9:49
  * //@Email: fansy1990@foxmail.com
  * 分割特征列和目标列
  * 由于前一个步骤输出的时候特征列和目标列都在一个Vector中，所以需要进行分割；
  */
object SplitFeatureWithLabel {

  def split(data:DataFrame) :DataFrame= {

    // Array of element names that need to be fetched
    // ArrayIndexOutOfBounds is not checked.
    // sizeof `elements` should be equal to the number of entries in column `features`
    val elements = Array(visit_duration, visit_count, visit_relevant_count, label)

    // Create a SQL-like expression using the array
    val sqlExpr = elements.zipWithIndex.map{ case (alias, idx) => col(features).getItem(idx).as(alias) }

    val de_features_label = data.select(col(userid) +: sqlExpr : _*)

    de_features_label
  }

def writeData(data:DataFrame,table:String)={
  val url = "jdbc:mysql://192.168.111.75:3306/law_fansy?useUnicode=true&characterEncoding=utf8"
  val user = "root"
  val password = "root"

  val driver ="com.mysql.jdbc.Driver"
  val  properties = new Properties()
  properties.put("user",user)
  properties.put("password",password)
  properties.put("driver",driver)
  data.write.mode(SaveMode.Append).jdbc(url,table,properties)

}
  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()

    val features_data = ConstructFeatures.getConstructFeatures(data)
//    println(features_data)
    val features_label_data = split(features_data)
    features_label_data.show(3)
//    println(features_label_data)

//    print(features_label_data.count())
    println("1类数据："+features_label_data.filter(label+"=1").count())
    println("0类数据："+features_label_data.filter(label+"=0").count())
    writeData(features_label_data.filter(label+"=1"),"law_fansy.gz_lawyer_data_1")
    writeData(features_label_data.filter(label+"=0"),"law_fansy.gz_lawyer_data_0")

  }
}

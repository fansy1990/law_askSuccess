package a_01_preprocess

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.DataFrame

import util.CommonUtils._
/**
  * //@Author: fansy 
  * //@Time: 2018/9/21 11:26
  * //@Email: fansy1990@foxmail.com
  */
object ScaleData {

  def scale(data:DataFrame):DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol(assembled_features)
      .setOutputCol(scaled_features)
      .setWithStd(true)
      .setWithMean(true)
    // 归一化后的数据
    val scaled_data = scaler.fit(data).transform(data)
    scaled_data
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
//    println(features_data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    features_label_data.show(3)
//    println(features_label_data)
    val filtered_data = FilterData.filterData(features_label_data)
//    filtered_data.show(2)
    val assembled_data =AssembleFeatureWithLabel.assemble(filtered_data)
    assembled_data.show(2)
    val scaled_data = scale(assembled_data)
//    println(scaled_data)
    scaled_data.show(3,false)
  }
}

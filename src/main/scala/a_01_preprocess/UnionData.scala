package a_01_preprocess

import org.apache.spark.sql.DataFrame
import util.CommonUtils._

/**created by suling
  * 用于将指定数量的0类数据与1类数据合并
  */
object UnionData {

  def unionData(data:DataFrame):DataFrame={
    val data1=data.filter(label+"=1")
    val size=data1.count()*0.5
    val data0=data.filter(label+"=0").limit(size.toInt)
    data0.union(data1)
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    val union_data=unionData(features_label_data)
    println("记录数"+union_data.count())
  }
}

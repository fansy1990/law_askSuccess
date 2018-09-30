package a_01_preprocess

import _root_.util.CommonUtils._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * //@Author: fansy 
  * //@Time: 2018/9/21 9:49
  * //@Email: fansy1990@foxmail.com
  * 合并所有特征列
  */
object AssembleFeatureWithLabel {

  def assemble(data:DataFrame) :DataFrame= {


   // assemble features
   val assembler = new VectorAssembler()
     .setInputCols(Array(visit_duration,visit_count,visit_relevant_count))
     .setOutputCol(assembled_features)
    val assemble_features_label = assembler.transform(data)
    assemble_features_label
  }

  def assemble2(data:DataFrame) :DataFrame= {


    // assemble features
    val assembler = new VectorAssembler()
      .setInputCols(Array(visit_duration,visit_count,visit_relevant_count,label))
      .setOutputCol("assembled2_features")
    val assemble_features_label = assembler.transform(data)
    assemble_features_label
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()

    val features_data = ConstructFeatures.getConstructFeatures(data)
//    println(features_data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
//    features_label_data.show(3)
    val filtered_data = FilterData.filterData(features_label_data)
//    println(features_label_data)
    val assembled_data = assemble(filtered_data)
//    print(features_label_data.count())
    assembled_data.show(3)
  }
}

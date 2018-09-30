package a_02_model

import a_01_preprocess._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame
import util.CommonUtils._

/**suling
  * 逻辑回归测试类
  */
object LogisticRegressionTest {

  def create_model(data:DataFrame)={

    val lr = new LogisticRegression().setLabelCol(label).setStandardization(true)
      .setFeaturesCol(scaled_features)
      .setPredictionCol(predict_column)
      .setThreshold(0.1)

    val model=lr.fit(data)
    model
  }

  def evaluate_model(data: DataFrame, model:LogisticRegressionModel ) = {

    val predictions = model.transform(data)
    // Select example rows to display.
    predictions.select(predict_column, label, scaled_features, userid).show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(label)
      .setPredictionCol(predict_column)
      .setMetricName("accuracy")

    val metrics = new MulticlassMetrics(predictions.select(predict_column, label).rdd.map(t => (t.getDouble(0),t.getDouble(1))))
    val accuracy = evaluator.evaluate(predictions)
    (accuracy,metrics)
  }

  def main(args: Array[String]): Unit = {
    val data = ReadHiveDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    //features_label_data.show(3)
    val features_change=DataExchange.change(features_label_data)
    val filtered_data = FilterData.filterData(features_change)
    val assembled_data =AssembleFeatureWithLabel.assemble(filtered_data)
    //assembled_data.show(2)
    val scaled_data = ScaleData.scale(assembled_data)
    //scaled_data.show(2,false)
    //
    println("scaled_data size :" + scaled_data.count())

    val Array(train,test) = scaled_data.randomSplit(Array(0.8,0.2))

    //    val model = create_model(scaled_data)
    //    val accuracy = evaluate_model(scaled_data, model)
    //    println("Test Error = " + (1.0 - accuracy))

    val model = create_model(train)
    val (accuracy,metrics) = evaluate_model(test, model)
    println("Test Error = " + (1.0 - accuracy))

    println(metrics.confusionMatrix)
    println("f:"+metrics.weightedFMeasure)
    println("precision:"+metrics.weightedPrecision)
    println("recall:"+metrics.weightedRecall)
    println("accuracy:"+metrics.accuracy)

    println("test.count:"+test.count())
    println("test.1.count:"+test.filter(label+" = 1").count)
  }
}

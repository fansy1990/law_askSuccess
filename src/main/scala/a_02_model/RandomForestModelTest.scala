package a_02_model

import a_01_preprocess._
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame
import util.CommonUtils._

/**
  * //@Author: fansy 
  * //@Time: 2018/9/21 13:46
  * //@Email: fansy1990@foxmail.com
  * 随机森林 测试类
  */
object RandomForestModelTest {

  def create_model(data: DataFrame) = {
    // Split the data into training and test sets (30% held out for testing).
    //    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol(label)
      .setFeaturesCol(scaled_features)
      .setNumTrees(11)
      .setPredictionCol(predict_column)

    //    // Convert indexed labels back to original labels.
    //    val labelConverter = new IndexToString()
    //      .setInputCol("prediction")
    //      .setOutputCol("predictedLabel")
    //      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    //    val pipeline = new Pipeline()
    //      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model = rf.fit(data)
    model
  }

  def evaluate_model(data: DataFrame, model:RandomForestClassificationModel ) = {

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
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    features_label_data.show(3)
    val filtered_data = FilterData.filterData(features_label_data)
    val assembled_data =AssembleFeatureWithLabel.assemble(filtered_data)
    assembled_data.show(2)
    val scaled_data = ScaleData.scale(assembled_data)
    scaled_data.show(2)
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
    println(metrics)

  }
}

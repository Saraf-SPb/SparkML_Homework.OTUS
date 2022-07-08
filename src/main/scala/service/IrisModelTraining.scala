package service

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

class IrisModelTraining(private val spark: SparkSession,
                        private val dataPath: String) {

  private val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  private val decision_tree_classifier: DecisionTreeClassifier = new DecisionTreeClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")

  private val pipeline: Pipeline = new Pipeline().setStages(Array(decision_tree_classifier))

  def training(fileName: String, outputFilePath: String): Unit = {
    val irisDF: DataFrame = spark.read
      .format("libsvm")
      .load(s"$dataPath/$fileName")

    val Array(train, test) = irisDF.randomSplit(Array(0.75, 0.25))
    val model = pipeline.fit(train)
    val predictions = model.transform(test)
    val accuracy = evaluator.evaluate(predictions)
    println(s"DONE. Accuracy: $accuracy")

    model.write.overwrite().save(outputFilePath)
  }
}

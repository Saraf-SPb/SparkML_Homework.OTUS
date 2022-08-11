package service

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{col, concat_ws, from_csv, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class IrisStructuredStreaming(private val spark: SparkSession,
                              private val modelPath: String,
                              private val kafka_bootstrap_server_url: String,
                              private val kafka_topic_output: String,
                              private val kafka_topic_input: String
                   ) extends java.io.Serializable {

  import spark.implicits._

  private val iris_labels = Map(0.0 -> "setosa", 1.0 -> "versicolor", 2.0 -> "virginica")

  private val prediction_value_udf = udf((col: Double) => iris_labels(col))

  private val iris_schema = StructType(
    StructField("sepal_length", DoubleType, nullable = true) ::
      StructField("sepal_width", DoubleType, nullable = true) ::
      StructField("petal_length", DoubleType, nullable = true) ::
      StructField("petal_width", DoubleType, nullable = true) ::
      Nil
  )

  private val vector_assembler = new VectorAssembler()
    .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
    .setOutputCol("features")

  def runStream(): Unit = {
    val model = PipelineModel.load(modelPath)

    val dataDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_server_url)
      .option("failOnDataLoss", "false")
      .option("subscribe", kafka_topic_input)
      .load()
      .select($"value".cast(StringType))
      .withColumn("struct", from_csv($"value", iris_schema, Map("sep" -> ",")))
      .withColumn("sepal_length", $"struct".getField("sepal_length"))
      .withColumn("sepal_width", $"struct".getField("sepal_width"))
      .withColumn("petal_length", $"struct".getField("petal_length"))
      .withColumn("petal_width", $"struct".getField("petal_width"))
      .drop("value", "struct")

    val data: DataFrame = vector_assembler.transform(dataDF)

    val prediction: DataFrame = model.transform(data)

    val query = prediction
      .withColumn(
        "predictedLabel",
        prediction_value_udf(col("prediction"))
      )
      .select(
        $"predictedLabel".as("key"),
        concat_ws(",", $"sepal_length", $"sepal_width", $"petal_length", $"petal_width", $"predictedLabel")
          .as("value")
      )
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_server_url)
      .option("checkpointLocation", "src/main/resources/checkpoint/")
      .option("topic", kafka_topic_output)
      .start()

    query.awaitTermination
  }
}

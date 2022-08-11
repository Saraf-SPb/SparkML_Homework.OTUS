import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{col, concat_ws, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import service.KafkaSink

import java.util.Properties

object Streaming extends App {

  val configFactory = ConfigFactory.load()

  val resources_path = configFactory.getString("config.resources_path")
  val model_path = resources_path + "model"
  val training_path = resources_path + "training"
  val kafka_bootstrap_server_url = configFactory.getString("config.kafka_bootstrap_server_url")
  val kafka_topic_output =configFactory.getString("config.kafka_topic_output")
  val kafka_topic_input = configFactory.getString("config.kafka_topic_input")
  val spark_executor_memory = configFactory.getString("config.spark_executor_memory")

  val iris_labels = Map(0.0 -> "setosa", 1.0 -> "versicolor", 2.0 -> "virginica")
  val prediction_value_udf = udf((col: Double) => iris_labels(col))

  // Создаём Streaming Context и получаем Spark Context
  val sparkConf = new SparkConf()
    .setAppName("Streaming")
    .setMaster("local[2]")
    .set("spark.executor.memory", spark_executor_memory)
  val streamingContext = new StreamingContext(sparkConf, Seconds(1))
  val sparkContext = streamingContext.sparkContext

  val model = PipelineModel.load(model_path)

  // Создаём свойства Producer'а для вывода в выходную тему Kafka (тема с расчётом)
  val props: Properties = new Properties()
  props.put("bootstrap.servers", kafka_bootstrap_server_url)

  // Создаём Kafka Sink (Producer)
  val kafkaSink = sparkContext.broadcast(KafkaSink(props))

  // Параметры подключения к Kafka для чтения
  val kafkaParams: Map[String, Object] = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafka_bootstrap_server_url,
    ConsumerConfig.GROUP_ID_CONFIG -> "group1",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
  )

  // Подписываемся на входную тему Kafka (тема с данными)
  val inputTopicSet = Set(kafka_topic_input)
  val messages: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](inputTopicSet, kafkaParams)
  )
  // Разбиваем входную строку на элементы
  val lines = messages
    .map(_.value)
    .map(_.replace("\"", "").split(","))

  lines.foreachRDD { rdd =>
    val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
    import spark.implicits._

    // Преобразовываем RDD в DataFrame
    val dataDF = rdd
      .toDF("input")
      .withColumn("sepal_length", $"input"(0).cast(DoubleType))
      .withColumn("sepal_width", $"input"(1).cast(DoubleType))
      .withColumn("petal_length", $"input"(2).cast(DoubleType))
      .withColumn("petal_width", $"input"(3).cast(DoubleType))
      .drop("input")

    if (dataDF.count() > 0) {
      val vector_assembler = new VectorAssembler()
        .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
        .setOutputCol("features")

      val data: DataFrame = vector_assembler.transform(dataDF)
      val prediction: DataFrame = model.transform(data)

      prediction
        .withColumn(
          "predictedLabel",
          prediction_value_udf(col("prediction"))
        )
        .select(
          $"predictedLabel".as("key"),
          concat_ws(",", $"sepal_length", $"sepal_width", $"petal_length", $"petal_width", $"predictedLabel")
            .as("value")
        )
        .foreach { row =>
          kafkaSink.value.send(kafka_topic_output,
            s"${row(1)}")
        }
    }
  }

  streamingContext.start()
  streamingContext.awaitTermination()

  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession.builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}


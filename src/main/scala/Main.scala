import org.apache.spark.sql.SparkSession
import service.{IrisModelTraining, IrisStreaming}

object Main extends App {

  val spark = SparkSession.builder()
    .appName("SparkML_Homework")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val resources_path = "src/main/resources/"
  val model_path = resources_path + "model"
  val training_path = resources_path + "training"
  val iris_libsvm_file_name = "iris_libsvm.txt"
  val kafka_bootstrap_server_url = "localhost:29092"
  val kafka_topic_output = "prediction"
  val kafka_topic_input = "input"

  if (args.length > 0 && args.head == "training") {
    val irisModelTraining = new IrisModelTraining(spark, training_path)
    irisModelTraining.training(iris_libsvm_file_name, model_path)
  }
  val irisStreaming = new IrisStreaming(spark,
    model_path,
    kafka_bootstrap_server_url,
    kafka_topic_output,
    kafka_topic_input)
  irisStreaming.runStream()
}

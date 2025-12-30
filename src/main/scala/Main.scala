import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestSpark")
      .master("local[*]")
      .getOrCreate()

    val df = spark.range(10)
    df.show()

    spark.stop()
  }
}

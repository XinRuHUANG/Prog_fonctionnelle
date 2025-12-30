package ingestion

import org.apache.spark.sql.{Dataset, SparkSession}
import model.PollutionRecord

object DataLoader {
  def loadCsv(path: String)(implicit spark: SparkSession): Dataset[PollutionRecord] = {
    import spark.implicits._
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .as[PollutionRecord]
  }
}

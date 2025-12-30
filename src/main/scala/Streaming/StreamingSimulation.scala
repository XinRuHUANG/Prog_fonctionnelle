package streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StreamingSimulation {

  def startStreaming(inputDir: String)(implicit spark: SparkSession): Unit = {

    println("\n=== Démarrage du streaming ===")

    val streamDF = spark.readStream
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputDir)

    val transformed = streamDF
      .withColumn("Heure", split(col("Horaire"), ":")(0).cast("int"))
      .withColumn("IndicePollution",
        col("`CO2 (ppm)`") / 500.0 +
          col("`Particules fines (µg/m3)`") / 80.0 +
          col("`Bruit (dB)`") / 90.0
      )
      .withColumn("Alerte",
        when(col("IndicePollution") > 2.0, "ALERTE POLLUTION")
          .otherwise("OK")
      )

    val query = transformed.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }
}

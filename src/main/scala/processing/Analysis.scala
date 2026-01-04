package processing

import org.apache.spark.sql.{DataFrame, functions => F}

object Analysis {

  def stationsLesPlusExposees(df: DataFrame): DataFrame =
    df.groupBy("Station")
      .agg(F.avg("CO2 (ppm)").as("CO2_moyen"))
      .orderBy(F.desc("CO2_moyen"))

  def picsHoraires(df: DataFrame): DataFrame = {
    // Convertir Horaire en timestamp (yyyy-MM-dd HH ou yyyy-MM-dd HH:mm:ss)
    val df2 = df.withColumn("HoraireTS", F.to_timestamp(F.col("Horaire"), "yyyy-MM-dd HH"))
      .withColumn("Heure", F.hour(F.col("HoraireTS")))

    // Calcul de la moyenne de CO2 par heure
    df2.groupBy("Heure")
      .agg(F.avg("CO2 (ppm)").as("CO2_moyen"))
      .orderBy(F.desc("CO2_moyen"))
  }

  def anomalies(df: DataFrame): DataFrame = {
    val withIndex = df.withColumn(
      "IndicePollution",
      F.col("CO2 (ppm)") / 500.0 +
        F.col("Particules fines (µg/m3)") / 80.0 +
        F.col("Bruit (dB)") / 90.0
    )

    val stats = withIndex.agg(
      F.avg("IndicePollution").as("moy"),
      F.stddev("IndicePollution").as("std")
    ).first()

    val moy = stats.getAs[Double]("moy")
    val std = stats.getAs[Double]("std")

    withIndex.filter(F.col("IndicePollution") > moy + 2 * std)
  }
}

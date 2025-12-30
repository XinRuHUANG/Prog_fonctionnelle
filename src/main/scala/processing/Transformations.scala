package processing

import org.apache.spark.sql.{DataFrame, Dataset, functions => F}
import model.PollutionRecord

object Transformations {

  def statsParStation(df: DataFrame): DataFrame =
    df.groupBy("Station")
      .agg(
        F.avg("CO2 (ppm)").as("CO2_moyen"),
        F.max("CO2 (ppm)").as("CO2_max"),
        F.min("CO2 (ppm)").as("CO2_min")
      )

  def statsParLigne(df: DataFrame): DataFrame =
    df.groupBy("Ligne de transport")
      .agg(
        F.avg("Particules fines (µg/m3)").as("PM_moyen"),
        F.max("Particules fines (µg/m3)").as("PM_max")
      )
}

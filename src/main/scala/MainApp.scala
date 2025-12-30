import org.apache.spark.sql.SparkSession

import ingestion.DataLoader
import processing._
import streaming.StreamingSimulation

object MainApp {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("ProjetPollutionTransport")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n=== Démarrage du projet Pollution & Transport ===\n")

    val path = "pollution_data.csv"

    val raw = DataLoader.loadCsv(path)
    val clean = raw.toDF()

    println("\n→ Statistiques par station :")
    Transformations.statsParStation(clean).show()

    println("\n→ Stations les plus exposées :")
    Analysis.stationsLesPlusExposees(clean).show()

    println("\n→ Construction du graphe des stations...")
    val graph = GraphModel.buildGraph(clean)
    println(s"Stations : ${graph.numVertices}")
    println(s"Connexions : ${graph.numEdges}")

    println("\n→ Prédiction CO2 :")
    val (_, predictions) = Prediction.trainModel(clean)
    predictions.select("Station", "Horaire", "CO2 (ppm)", "prediction").show(20, false)

    println("\n=== Fin du programme ===\n")

    spark.stop()
  }
}

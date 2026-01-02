import org.apache.spark.sql.SparkSession

import ingestion.DataLoader
import processing._
import streaming.StreamingSimulation

object MainApp {

  def main(args: Array[String]): Unit = {

    // --- Création unique de SparkSession pour tout le projet ---
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("ProjetPollutionTransport")
      .master("local[*]")

      // ✅ Utiliser JavaSerializer pour éviter les problèmes Kryo avec Java 21 + GraphX
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

      // ✅ Ouvrir les modules Java nécessaires pour Java 21
      .config(
        "spark.driver.extraJavaOptions",
        "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
      )
      .config(
        "spark.executor.extraJavaOptions",
        "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
      )
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n=== Démarrage du projet Pollution & Transport ===\n")

    val path = "H:\\Desktop\\ProgFonctV\\data\\pollution_data.csv"

    // --- Lecture du CSV directement via Spark ---
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    df.show(5)
    df.printSchema()

    // --- Chargement via DataLoader (utilise SparkSession implicite) ---
    val raw = DataLoader.loadCsv(path)
    val clean = raw.toDF()

    println("\n→ Statistiques par station :")
    Transformations.statsParStation(clean).show()

    println("\n→ Stations les plus exposées :")
    Analysis.stationsLesPlusExposees(clean).show()

    println("\n→ Pics Horaires :")
    Analysis.picsHoraires(clean).show()

    println("\n-> Anomalies :")
    Analysis.anomalies(clean).show()

    //println("\n→ Construction du graphe des stations...")
    //val graph = GraphModel.buildGraph(clean)
    //println(s"Stations : ${graph.numVertices}")
    //println(s"Connexions : ${graph.numEdges}")

    println("\n→ Prédiction CO2 :")
    val (_, predictions) = Prediction.trainModel(clean)
    predictions
      .select("Station", "Horaire", "CO2 (ppm)", "prediction")
      .show(20, false)

    println("\n=== Fin du programme ===\n")

    spark.stop()
  }
}

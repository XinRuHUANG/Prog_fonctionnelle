package processing

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Prediction {

  def prepareFeatures(df: DataFrame): DataFrame = {

    val categoricalCols = Seq("Station", "Ligne de transport", "Meteo", "Jour de la semaine", "Affluence")

    val indexers = categoricalCols.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "_idx")
        .setHandleInvalid("keep")
    }

    val encoders = categoricalCols.map { colName =>
      new OneHotEncoder()
        .setInputCol(colName + "_idx")
        .setOutputCol(colName + "_vec")
    }

    // ✅ Transformer Horaire en timestamp puis extraire l'heure
    val df2 = df.withColumn(
      "HoraireTS",
      to_timestamp(col("Horaire"), "yyyy-MM-dd HH")
    ).withColumn("Heure", hour(col("HoraireTS")))

    val numericCols = Seq("Heure", "Particules fines (µg/m3)", "Bruit (dB)", "Humidite (%)")

    val featureCols = categoricalCols.map(_ + "_vec") ++ numericCols

    val assembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol("features")

    val pipeline = new Pipeline().setStages((indexers ++ encoders :+ assembler).toArray)

    pipeline.fit(df2).transform(df2)
  }

  def trainModel(df: DataFrame): Map[String, DataFrame] = {

    val prepared = prepareFeatures(df)
    
    val models = Seq(
      "LinearRegression" -> new LinearRegression()
        .setLabelCol("CO2 (ppm)")
        .setFeaturesCol("features"),

      "DecisionTree" -> new DecisionTreeRegressor()
        .setLabelCol("CO2 (ppm)")
        .setFeaturesCol("features"),

      "RandomForest" -> new RandomForestRegressor()
        .setLabelCol("CO2 (ppm)")
        .setFeaturesCol("features")
        .setNumTrees(50)
    )

    // Entraînement + prédictions
    models.map { case (name, algo) =>
      val model = algo.fit(prepared)
      val predictions = model.transform(prepared)
      name -> predictions
    }.toMap
  }
}

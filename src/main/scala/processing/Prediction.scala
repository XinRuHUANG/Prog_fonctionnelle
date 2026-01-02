package processing

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Prediction {

  def buildFeaturePipeline(categoricalCols: Seq[String], numericCols: Seq[String]): Pipeline = {

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

    val featureCols = categoricalCols.map(_ + "_vec") ++ numericCols

    val assembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol("features")

    new Pipeline().setStages((indexers ++ encoders :+ assembler).toArray)
  }

  def prepareData(df: DataFrame): DataFrame = {

    val df2 = df.withColumn("Heure", split(col("Horaire"), ":")(0).cast("int"))

    df2
  }

  def trainModels(df: DataFrame): Map[String, DataFrame] = {

    val categoricalCols = Seq("Station", "Ligne de transport", "Meteo", "Jour de la semaine", "Affluence")
    val numericCols = Seq("Heure", "Particules fines (µg/m3)", "Bruit (dB)", "Humidite (%)")

    val pipeline = buildFeaturePipeline(categoricalCols, numericCols)

    val prepared = prepareData(df)

    // Fit du pipeline de features
    val featureModel = pipeline.fit(prepared)
    val featuredDF = featureModel.transform(prepared)

    // Définition des modèles MLlib
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
      val model = algo.fit(featuredDF)
      val predictions = model.transform(featuredDF)
      name -> predictions
    }.toMap
  }
}

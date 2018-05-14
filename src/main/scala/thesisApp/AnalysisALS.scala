package thesisApp

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class AnalysisALS {

  def analisysALS(df: DataFrame): (DataFrame, Double) = {
    println("\n\n>>> Análisis tipo recomendador basado en ALS\n")

    println("Dividiendo el dataset en entrenamiento y prueba:")
    val Array(training, test) = df.toDF("user", "item", "rating").randomSplit(Array(0.8, 0.2))
    println("Entradas del set de entrenamiento: " + training.count())
    println("Entradas del set de prueba: " + test.count())

    println("\nCreando el modelo ALS.")
    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(1)
      .setRegParam(0.01)
      .setUserCol("user")
      .setItemCol("item")
      .setRatingCol("rating")
    println("Entrenando el modelo ALS.")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")

    println("Generando predicciones sobre el dataset de prueba.")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    println("\n Evaluando modelo:")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error (RMSE) sobre el dataset de prueba) = $rmse")


    //    println("Midiendo recomendaciones sobre el dataset de entrenamiento.")
    //    val predictionsTr = model.transform(training)
    //    val rmseTr = evaluator.evaluate(predictionsTr)
    //    println(s"Root-mean-square error (en el dataset de entrenamiento) = $rmseTr")


    // Generate top 10 product recommendations for each user
    val top10recommendations = model
      .recommendForAllUsers(10)
      .withColumn("recommendations", explode(col("recommendations")))
      .select(col("user"), col("recommendations").getField("item"), col("recommendations").getField("rating"))
      .toDF(df.columns.toList: _*)
    (top10recommendations, rmse)
  }
}

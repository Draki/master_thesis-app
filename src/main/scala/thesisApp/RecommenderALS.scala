// args(analysisName, sourceFile, baseDir, numClients, numProds, outputMode)
// args("clientIndex"/"transac", "DelightingCustomersBDextract2.json", "./src/main/scala/", 10, 10, "oneJSON")

package thesisApp

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.util.parsing.json.JSON


object RecommenderALS {
  def main(args: Array[String]) {

    var baseDir = "./src/main/scala/"
    var configFile = "recommenderALS_sample"
    if (args.length > 0)  baseDir = args(0)
    if (args.length > 1)  configFile = args(1)
    val readDir = baseDir + "data/"


    // Loading configuration from file or defaults
    val configMap: Map[String, String] = JSON.parseFull(readDir + configFile) match {
      case Some(e: Map[String, String]) => e
      case _ => Map()
    }
    val appName = configMap.getOrElse("appName", "RecommenderALS")
    val sourceFile = configMap.getOrElse("sourceFile", "DelightingCustomersBDextract2.json")
    val outputMode = configMap.getOrElse("outputMode", "oneJSON")
    val numClients = configMap.getOrElse("numClients", "10").toInt
    val numProds = configMap.getOrElse("numProds", "10").toInt
    val usersCol = configMap.getOrElse("usersCol", "clientIndex")
    val itemsCol = configMap.getOrElse("itemsCol", "prodNameIndex")
    val ratingsCol = configMap.getOrElse("ratingsCol", "prodUds")

    val resultsDir = "%sresults/%s%s/".format(
      baseDir,
      sourceFile.replace(".json", "/"),
      LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH.mm.ss"))
    )
    new File(resultsDir).mkdirs()
    val timeLogPath = resultsDir + "timeLog.json"


    // Starting SparkSession
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder
      .appName(appName)
      .master("local")
      .getOrCreate()

    // Loading modules
    val utilities = new UtilsCarrefourDataset()


    // Formatting file to the standar JSON format managed by Spark
    var timerModule = System.currentTimeMillis
    val formattedFile = utilities.fileFormatter(readDir, sourceFile, baseDir + "results/")
    utilities.timeLogger("fileFormatter(.JSON)", numClients, numProds, timerModule, timeLogPath)

    // Loading formatted file as a dataframe table
    val (spartans, clientConverter, clientIndex, productConverter, prodNameIndex) =
      utilities.tableLoader(formattedFile, spark, numClients, numProds)

    // Executing data exploration
    timerModule = System.currentTimeMillis
    analisysALS(
      spartans
        .groupBy(itemsCol, usersCol)
        .agg(functions.sum(ratingsCol).as(ratingsCol))
    )
    utilities.timeLogger(appName +"(" + itemsCol +"," + usersCol + ")_rated_by_" + ratingsCol, numClients, numProds, timerModule, timeLogPath)

    def analisysALS(df: DataFrame): Unit = {

      val cols = df.columns.toList
      val colDEIndexified = cols.map(x => x.replace("Index", ""))

      println("\n\n>>> Análisis tipo recomendador basado en ALS\n")
      val analyserALS = new AnalysisALS()
      val (modelClients, rmse) = analyserALS.analysisALS(df)

      // Generate top 10 product recommendations for each user
      var clientRecs = modelClients
        .recommendForAllUsers(10)
        .withColumn("recommendations", explode(col("recommendations")))
        .select(col("user"), col("recommendations").getField("item"), col("recommendations").getField("rating"))
        .toDF(cols: _*)

      if (cols.contains(clientIndex)) {
        clientRecs = clientConverter.transform(clientRecs)
      }
      if (cols.contains(prodNameIndex)) {
        clientRecs = productConverter.transform(clientRecs)
      }

      // Translate back elements and aggregate recommendations
      val convertedClientRecs = clientRecs
        .orderBy(desc(colDEIndexified(2)))
        .groupBy(colDEIndexified(0))
        .agg(collect_set(struct(colDEIndexified(1), colDEIndexified(2))).alias("recomendations"))

      println("\n\n>>> Guardando Top 10 recomendaciones por usuario\n")
      utilities.printFile(convertedClientRecs, resultsDir, appName + "(" + cols(0) + "," + cols(1) + ")_by_" + cols(2) + "_rmse_" + rmse, outputMode)
    }
  }
}


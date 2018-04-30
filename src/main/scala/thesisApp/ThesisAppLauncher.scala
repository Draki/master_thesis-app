package thesisApp

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.util.parsing.json.JSON

// args(sourcefile, outputDir, *recommenderFiles)
object ThesisAppLauncher {
  def main(args: Array[String]) {

    val sourceFile = if (args.length > 0) args(0) else "./data/DelightingCustomersBDextract2.json"
    val outputDir = if (args.length > 1) args(1) else "./results/"
    val analisysList = if (args.length > 2) args.toList.drop(2) else List("default")

    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH.mm.ss"))
    new File(outputDir).mkdirs()

    // Loading modules
    val utilities = new UtilsCarrefourDataset()

    // Formatting file to the standar JSON format managed by Spark
    var timerModule = System.currentTimeMillis
    val formattedFile = utilities.fileFormatter(sourceFile, outputDir)

    // Starting SparkSession
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder
      .appName("TestAppLauncher")
            .master("local")
      .getOrCreate()

    // Loading formatted file as a dataframe table
    val flatTable = utilities.tableLoader(formattedFile, spark)

    val (clientIndexer, clientConverter, clientIndex) = utilities.columnIndexer(flatTable, "client")
    val (prodNameIndexer, productConverter, prodNameIndex) = utilities.columnIndexer(flatTable, "prodName")
    val spartanspre = prodNameIndexer.transform(clientIndexer.transform(flatTable))


    for (analysisConf <- analisysList) {


      // Loading configuration from file or defaults
      val configJson = if (analysisConf == "default") { "" }
      else { JSON.parseFull(scala.io.Source.fromFile(analysisConf).mkString) }

      val configMap: Map[String, String] = configJson match {
        case Some(e: Map[String, String] @unchecked) => e
        case _ => Map()
      }
      val appName = configMap.getOrElse("appName", "DataExplorer")
      utilities.setOutputMode(configMap.getOrElse("outputMode", "oneJSON"))
      val numClients = configMap.getOrElse("numClients", "10").toInt
      val numProds = configMap.getOrElse("numProds", "10").toInt
      val resultsDir = "%s/%s/%s/".format(
        outputDir,
        sourceFile.split("/").last.replace(".json", ""),
        timestamp)

      new File(resultsDir).mkdirs()
      val timeLogPath = resultsDir + "timeLog.json"

      // Taking the top clients and products
      val spartansMid = utilities
        .filterAmountCols(spartanspre, clientIndex, numClients)
      val spartans = utilities
        .filterAmountCols(spartansMid, prodNameIndex, numProds)


      // Executing analysis
      timerModule = System.currentTimeMillis
      val saveName = appName match {
        case "DataExplorer" => dataExplorer(appName, spartans, resultsDir, utilities)
        case "RecommenderALS" => recommenderALS(appName, configMap, spartans, resultsDir, utilities, clientConverter, productConverter)
        case "RecommenderGraphD" => recommenderGraphD(appName, configMap, spartans, resultsDir, utilities)
        case "RecommenderGraphX" => recommenderGraphX(appName, configMap, spartans, resultsDir, utilities, spark)
        case _ => "WRONG CONFIG FILE FOR ANALYSIS: " + analysisConf
      }
      utilities.timeLogger(saveName, numClients, numProds, timerModule, timeLogPath)
    }

  }
  def dataExplorer(appName:String, df:DataFrame, resultsDir:String, utilities:UtilsCarrefourDataset): String = {
    val explorer = new DataExploration()
    explorer.dataExploration(appName, df, resultsDir, utilities)
    appName
  }


  def recommenderALS(appName:String, configMap: Map[String, String], df:DataFrame, resultsDir:String, utilities:UtilsCarrefourDataset, clientConverter:IndexToString, productConverter:IndexToString): String = {
    // Loading configuration from file or defaults
    val usersCol = configMap.getOrElse("usersCol", "clientIndex")
    val itemsCol = configMap.getOrElse("itemsCol", "prodNameIndex")
    val ratingsCol = configMap.getOrElse("ratingsCol", "prodUds")

    // Executing ALS analysis
    val analizer = new AnalysisALS()

    var (clientRecs, rmse) = analizer.analisysALS(
      df.groupBy(itemsCol, usersCol)
        .agg(functions.sum(ratingsCol).as(ratingsCol))
    )

    val cols = clientRecs.columns.toList
    if (cols.contains("clientIndex")) { clientRecs = clientConverter.transform(clientRecs) }
    if (cols.contains("prodNameIndex")) { clientRecs = productConverter.transform(clientRecs) }
    val colDEIndexified = cols.map(x => x.replace("Index", ""))

    // Translate back elements and aggregate recommendations
    val convertedClientRecs = clientRecs
      .orderBy(desc(colDEIndexified(2)))
      .groupBy(colDEIndexified(0))
      .agg(collect_set(struct(colDEIndexified(1), colDEIndexified(2))).alias("recommendations"))

    utilities.printFile(convertedClientRecs, resultsDir, appName + "(" + cols(0) + "," + cols(1) + ")_by_" + cols(2) + "_rmse_" + rmse)
    appName + "(" + itemsCol + "," + usersCol + ")_rated_by_" + ratingsCol
  }


  def recommenderGraphD(appName:String, configMap: Map[String, String], df:DataFrame, resultsDir:String, utilities:UtilsCarrefourDataset): String = {
    // Loading configuration from file or defaults
    val vertexCol = configMap.getOrElse("vertexCol", "prodName")
    val edgeCol = configMap.getOrElse("edgeCol", "clientIndex")

    // Executing graph analysis
    val graphAnalyzer = new AnalysisGraphD()
    graphAnalyzer.analysisGraphD(
      df.select(vertexCol, edgeCol),
      resultsDir, utilities
    )

    appName + "(" + vertexCol + "," + edgeCol + ")"
  }


  def recommenderGraphX(appName:String, configMap: Map[String, String], df:DataFrame, resultsDir:String, utilities:UtilsCarrefourDataset, spark:SparkSession): String = {
    // Loading configuration from file or defaults
    val vertexCol = configMap.getOrElse("vertexCol", "prodNameIndex")
    val vertexPropertiesCol = configMap.getOrElse("vertexPropertiesCol", "prodName")
    val edgeCol = configMap.getOrElse("edgeCol", "clientIndex")

    // Executing graph analysis
    val graphAnalyzer = new AnalysisGraphX()
    graphAnalyzer.analysisGraphX(
      df.groupBy(vertexCol)
        .agg(first(vertexPropertiesCol).as(vertexPropertiesCol)),
      df.select(vertexCol, edgeCol),
      resultsDir, utilities, spark
    )
    appName + "(" + vertexCol + "," + edgeCol + ")"
  }
}

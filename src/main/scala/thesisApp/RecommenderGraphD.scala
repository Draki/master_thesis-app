// args(appName, sourceFile, baseDir, numClients, numProds, outputMode)
// args("analyzer", "./src/main/scala/", "DelightingCustomersBDextract2.json", 10, 10, "parallelWriteJSON"/"oneJSON"/"standarOutput"

package thesisApp

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON


object RecommenderGraphD {
  def main(args: Array[String]) {

    var baseDir = "./"
    var configFile = "recommenderGraphD_sample"
    if (args.length > 0)  baseDir = args(0)
    if (args.length > 1)  configFile = args(1)
    val readDir = baseDir + "data/"


    // Loading configuration from file or defaults
    val configMap: Map[String, String] = JSON.parseFull(readDir + configFile) match {
      case Some(e: Map[String, String] @unchecked) => e
      case _ => Map()
    }
    val appName = configMap.getOrElse("appName", "RecommenderGraphD")
    val sourceFile = configMap.getOrElse("sourceFile", "DelightingCustomersBDextract2.json")
    val outputMode = configMap.getOrElse("outputMode", "oneJSON")
    val numClients = configMap.getOrElse("numClients", "10").toInt
    val numProds = configMap.getOrElse("numProds", "10").toInt
    val vertexCol = configMap.getOrElse("vertexCol", "prodName")
    val edgeCol = configMap.getOrElse("edgeCol", "clientIndex")

    val resultsDir = "%sresults/%s%s/".format(
      baseDir,
      sourceFile.replace(".json", "/"),
      LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH.mm.ss"))
    )
    new File(resultsDir).mkdirs()
    val timeLogPath = baseDir + "results/timeLog.json"


    // Starting SparkSession
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder
      .appName(appName)
      .getOrCreate()

    // Loading modules
    val utilities = new UtilsCarrefourDataset()


    // Formatting file to the standar JSON format managed by Spark
    var timerModule = System.currentTimeMillis
    val formattedFile = utilities.fileFormatter(readDir, sourceFile, baseDir + "results/")
    utilities.timeLogger("fileFormatter(.JSON)",numClients, numProds, timerModule, timeLogPath)

    // Loading formatted file as a dataframe table
    val (spartans, clientConverter, clientIndex, productConverter, prodNameIndex) =
      utilities.tableLoader(formattedFile, spark, numClients, numProds)

    // Executing graph analysis
    timerModule = System.currentTimeMillis

    val graphAnalyzer = new AnalysisGraphD()
    graphAnalyzer.analysisGraphD(
      spartans.select("prodName", "clientIndex"),
      resultsDir, utilities, outputMode
    )

    utilities.timeLogger(appName + "(" + vertexCol + "," + edgeCol + ")", numClients, numProds, timerModule, timeLogPath)
  }
}


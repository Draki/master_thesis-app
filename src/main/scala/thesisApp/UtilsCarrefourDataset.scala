package thesisApp

import java.io._
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime

import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class UtilsCarrefourDataset() {

  def fileFormatter(originalFilePath: String, writeDir: String): String = {
    val originalFile = new File(originalFilePath)
    val finalFilePath = writeDir + originalFile.getName.replace(".json", "Formatted.json")
    if (!Files.exists(Paths.get(finalFilePath))) {
      println("Formateando el archivo BSON (de MongoDB) a líneas JSON")
      print("Paso 1: Formato intermedio...")
      val filemedium = "auxiliarFile.json"
      val reader = new BufferedReader(new InputStreamReader(new FileInputStream(originalFile)))
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(writeDir + filemedium))))
      try {
        var content = reader.readLine()
        while (content != null) {
          content = content
            .replace("NumberLong(", "")
            .replace("NumberInt(", "")
            .replace("ISODate(", "")
            .replace(")", "")
            .trim
            .replace(",", ", ")
            .replace("{", "{\n")
          writer.write(content)
          writer.flush()
          content = reader.readLine()
        }
      } finally {
        reader.close()
        writer.close()
      }
      print("done\n" +
        "Paso 2: Formato JSON.........")
      val reader2 = new BufferedReader(new InputStreamReader(new FileInputStream(new File(writeDir + filemedium))))
      val writer2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(finalFilePath))))
      try {
        var content = reader2.readLine()
        while (content != null) {
          content = content
            .replace("\n", "")
            .replace("}{", "}\n{")
          writer2.write(content)
          writer2.flush()
          content = reader2.readLine()
        }
      } finally {
        reader2.close()
        writer2.close()
      }
      new File(writeDir + filemedium).delete()
      println("done")
    }
    finalFilePath
  }

  def tableLoader(formattedFile: String, spark: SparkSession): Dataset[Row] = {
    val schema = StructType(Seq(
      StructField("_id", IntegerType, nullable = false),
      StructField("client", LongType, nullable = false),
      StructField("date", DateType, nullable = false),
      StructField("items", ArrayType(
        StructType(Seq(
          StructField("desc", StringType, nullable = false),
          StructField("n_unit", FloatType, nullable = false),
          StructField("net_am", FloatType, nullable = false)
        ))
      ), nullable = false),
      StructField("mall", IntegerType, nullable = false))
    )

    val table = spark.read
      .schema(schema)
      .option("mode", "DROPMALFORMED")
      .json(formattedFile)

    table
      .filter("client is not null")
      .withColumn("items", explode(col("items")))
      .select(
        col("_id").as("transac"),
        col("client"),
        col("date"),
        col("items").getField("desc").as("prodName"),
        col("items").getField("n_unit").as("prodUds"),
        col("items").getField("net_am").as("prodsCost"),
        col("mall"))
      .filter(col("prodName") =!= "BOLSA CARREFOUR") // No aporta demasiado
  }

  def columnIndexer(df: DataFrame, colName: String): (StringIndexerModel, IndexToString, String) = {
    val indexColName = colName + "Index"
    //    By default, this is ordered by label frequencies so the most frequent label gets index 0.
    val indexer = new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(indexColName).fit(df)

    val deindexer = new IndexToString()
      .setInputCol(indexColName)
      .setOutputCol(colName)
      .setLabels(indexer.labels)

    (indexer, deindexer, indexColName)
  }

  def filterAmountCols(df: DataFrame, colName: String, numCols: Int): DataFrame = {
    if (colName.isEmpty) return df
    println("Filtrando el dataset por \"" + colName + "\" según los " + numCols + " valores más frecuentes")
    df.filter(col(colName) < numCols)
  }

  def timeLogger(module: String, clients: Int, products: Int, start: Long, filePath: String): Unit = {
    val bw = new BufferedWriter(new FileWriter(filePath, true))
    try {
      bw.write(
        "{\"timestamp\" : " + LocalDateTime.now() + "," +
          " \"module\" : " + module + "," +
          " \"clients\" : " + clients + "," +
          " \"products\" : " + products + "," +
          " \"processing_time\" : " + (System.currentTimeMillis - start) / 1000 + "}\n")
    }
    finally bw.close()
  }

  var outputMode = "oneJSON"

  def setOutputMode(mode: String): Unit = {outputMode = mode}

  def printFile(df: DataFrame, resultsDir: String, fileName: String): Unit = {
    outputMode match {
      case "parallelWriteJSON" => df.write.mode("append").json(resultsDir + fileName)
      case "oneJSON" =>
        val bw = new BufferedWriter(new FileWriter(new File(resultsDir + fileName + ".json")))
        try bw.write(df.toJSON.collect()(0))
        finally bw.close()
      case "standarOutput" => df.show()
      case _ => sys.error("Please type the right outputMode: \"parallelWriteJSON\"/\"oneJSON\"/\"standarOutput\"\n")
    }
  }
}

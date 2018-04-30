package thesisApp

import org.apache.spark.sql.functions.{col, count, lit, max, _}
import org.apache.spark.sql.{DataFrame, _}

class AnalysisGraphD {

  def analysisGraphD(df: DataFrame, resultsDir: String, utilities: UtilsCarrefourDataset, dampingFactor:Double = 0.85, tolerance:Double = 0.01): Unit = {

    val Seq(vertexName, edgeName) = df.columns.toSeq
    val edgeGenerator = df.distinct()

    val edge = edgeGenerator.toDF("orig", "link")
      .join(edgeGenerator.toDF("dest", "link"),"link")
      .groupBy("orig", "dest")
      .agg(count("link").as("linkWeight"))
      .filter(col("orig") =!= col("dest"))

    val strongestLinks = edge
      .withColumn("_A", functions.when(col("orig").lt(col("dest")), col("orig")).otherwise(col("dest")))
      .withColumn("_B", functions.when(col("orig").lt(col("dest")), col("dest")).otherwise(col("orig")))
      .select(col("_A").as("orig"),col("_B").as("dest"), col("linkWeight"))
      .distinct()
      .orderBy(desc("linkWeight"))
    println("Calculating strongestLinks")
    utilities.printFile(strongestLinks, resultsDir, "GraphD(" + vertexName + "," + edgeName + ")_strongestLinks")

    val vertexNumNeighbours = edge
      .groupBy("orig").agg(count("orig").as("neighbours"))
      .orderBy(desc("neighbours"))
    println("Calculating vertexwithMostNeighbours")
    utilities.printFile(vertexNumNeighbours, resultsDir, "GraphD(" + vertexName + "," + edgeName + ")_vertexNumNeighbours")

    val vertexOutDegree = edge
      .groupBy("orig").agg(sum("linkWeight").as("linksOfVertex"))
      .orderBy(desc("linksOfVertex"))
    println("Calculating vertexWithMostOutLinks")
    utilities.printFile(vertexOutDegree, resultsDir, "GraphX(" + vertexName + "," + edgeName + ")_vertexOutDegree")

    var iteratorDF = edge
      .join(vertexOutDegree, "orig")
      .withColumn("propagateWeight", (col("linkWeight") * dampingFactor) / col("linksOfVertex"))
      .select("orig","dest", "propagateWeight")
      .withColumn("rank", lit(1.0))

    var vuelta = 0
    var tol = 1.0

    while (tol > tolerance) {
      val rankAdder = iteratorDF
        .withColumn("rankExteralAddition", col("propagateWeight")*col("rank"))
        .groupBy("dest").agg(sum("rankExteralAddition"))
        .select(
          col("dest").as("orig"),
          col("sum(rankExteralAddition)").as("rankExteralAddition")
        )

      iteratorDF = iteratorDF.join(rankAdder,"orig")
        .withColumn("oldRank", col("rank"))
        .withColumn("rank", (col("rank")*(1-dampingFactor))+ col("rankExteralAddition"))
        .withColumn("tolerance", abs(col("oldRank")-col("rank")))
        .drop("rankExteralAddition")

      tol = iteratorDF.agg(max("tolerance")).head().getDouble(0)

      vuelta += 1
      println("vuelta: " + vuelta + ", tolerance: " + tol + ", iteratorDF size: " + iteratorDF.count())
    }

    val rankedVertex = iteratorDF
      .select("orig","rank")
      .distinct()
      .orderBy(desc("rank"))

    println("Calculating pageRank")
    utilities.printFile(rankedVertex, resultsDir, "GraphD(" + vertexName + "," + edgeName + ").pageRank(dampFact_" + dampingFactor + ",tol_"+ tolerance + ")_rankedVertex")
  }
}

package thesisApp

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
class AnalysisGraphX {

  def analysisGraphX(dfVertex: DataFrame,dfEdgeGenerator: DataFrame, spark: SparkSession, dampingFactor:Double = 0.85, tolerance:Double = 0.01): Unit = {

    val Seq(vertexIndex, vertexName) = dfVertex.columns.toSeq
    val Seq(vertexNameB, edgeName) = dfEdgeGenerator.columns.toSeq

    val vertex = dfVertex.rdd.map(row => (row(0).asInstanceOf[Number].longValue, row(1)))

    val edgeGenerator = dfEdgeGenerator.distinct()

    val edge = edgeGenerator.toDF("orig", "link")
      .join(edgeGenerator.toDF("dest", "link"), "link")
      //      .groupBy("orig", "dest").agg(count("link").as("weight"))
      .select("orig", "dest").withColumn("links", lit(1))
      .filter(col("orig") =!= col("dest"))
      .rdd
      .map(row => Edge(
        row(0).asInstanceOf[Number].longValue,
        row(1).asInstanceOf[Number].longValue,
        row(2).asInstanceOf[Number].longValue
      ))

    val graph = Graph(vertex, edge)

    println("Total Number of vertex(" + vertexName + "): " + g.numVertices)
    println("Total Number of enlaces(" + edgeName + "): " + g.numEdges)

    val schemaGroupEdges =  StructType(Seq(
      StructField(vertexName+"_orig", dataType = StringType, nullable = false),
      StructField(vertexName+"_dest", dataType = StringType, nullable = false),
      StructField("num_of_"+edgeName, dataType = IntegerType, nullable = false)))
    spark.createDataFrame(
      graph.groupEdges((edge1, edge2) => edge1 + edge2)
      .triplets
      .sortBy(_.attr, false)
      .map(triplet => Row(triplet.srcAttr.toString, triplet.dstAttr.toString, triplet.attr.toInt)),
    schemaGroupEdges).show()
//        "There are " + triplet.attr.toString + " common edges between the vertex" + triplet.srcAttr + " and " + triplet.dstAttr + ".")
//      .take(10)
//      .foreach(println)

//    ((8,MANDARINA MALLA 2),(0,PLÁTANO 1ª BOLSA),1)
//    ((8,MANDARINA MALLA 2),(1,NARANJA ZUMO CARRE),1)
//    ((8,MANDARINA MALLA 2),(2,BAGUETTE CARREFOUR),1)


    val schemaInDegrees =  StructType(Seq(
  StructField(vertexName, dataType = StringType, nullable = false),
  StructField("inDegree", dataType = IntegerType, nullable = false)))

    spark.createDataFrame(
      graph
      .inDegrees // computes in Degrees
      .join(vertex)
      .sortBy(_._2._1, false)
      .map(x => Row(x._2._2.toString, x._2._1.toInt)),
  schemaInDegrees).show()

//      .take(10)
//      .foreach(x => println(x._2._2 + " has " + x._2._1 + " in degrees."))
//      .foreach(println)

//    PLÁTANO 1ª BOLSA has 45 in degrees.
//      PAN PISTOLA/BARRA has 39 in degrees.
//      BAGUETTE CARREFOUR has 36 in degrees.



    val schemaPageRank =  StructType(Seq(
      StructField(vertexName, dataType = StringType, nullable = false),
      StructField("rank", dataType = DoubleType, nullable = false)))

    println("\n\n>>> Top 10 recomendaciones:\n")
    val results = graph.pageRank(tolerance, 1-dampingFactor).vertices
    spark.createDataFrame(
      results
      .join(vertex)
      .sortBy(_._2._1, false) // sort by the pageRank
        .map(x => Row(x._2._2.toString, x._2._1.toDouble)),
      schemaPageRank).show()

//      .take(10) // get the top 10
//      .foreach(println)
//      .foreach(x => println(x._2._2))
  }
}

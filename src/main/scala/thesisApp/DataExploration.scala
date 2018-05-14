package thesisApp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

class DataExploration {

  def dataExploration(appName:String, df: DataFrame, resultsDir: String, utilities: UtilsCarrefourDataset): Unit = {

    val totals = df
      .agg(
        countDistinct("clientIndex").as("total_clients"),
        countDistinct("prodNameIndex").as("total_different_products"),
        bround(sum("prodUds")).as("total_products_sold"),
        bround(sum("prodsCost"), 2).as("total_earnings"),
        countDistinct("transac").as("total_transactions")
      )

    val Row(totalItemsSold, totalDifferentItems, totalEarns, totalTransactions) = totals
      .select("total_products_sold","total_different_products","total_earnings", "total_transactions").collect()(0)
    utilities.printFile(totals, resultsDir, appName + "_totals")


    val totalsByProd = df.groupBy("prodName")
      .agg(
        bround(avg(col("prodsCost") / col("prodUds")), 2).as("avgPrice"),
        bround(sum("prodUds")).as("udsSold"),
        bround(sum("prodsCost"), 2).as("earnings"),
        countDistinct("transac").as("transacWithProd")
      )
    utilities.printFile(totalsByProd, resultsDir, appName + "_totalsByProd")

    val prodMasVendidos = totalsByProd
      .select("prodName", "udsSold")
      .withColumn("percentUdsSold", bround(col("udsSold") * 100 / totalItemsSold, 2))
      .orderBy(desc("udsSold"))
    utilities.printFile(prodMasVendidos, resultsDir, appName + "_prodsMostSold_totalUds-" + totalItemsSold)

    val prodMasGanancias = totalsByProd
      .select("prodName", "earnings")
      .withColumn("percentEarning", bround(col("earnings") * 100 / totalEarns, 2))
      .orderBy(desc("earnings"))
    utilities.printFile(prodMasGanancias, resultsDir, appName + "_prodsMostEarns_totalEarns-" + totalEarns)

    val prodsMostPopular = totalsByProd
      .select("prodName", "transacWithProd")
      .withColumn("percentTransacWithProd", bround(col("transacWithProd") * 100 / totalTransactions, 2))
      .orderBy(desc("transacWithProd"))
    utilities.printFile(prodsMostPopular, resultsDir, appName + "_prodsMostPopular_totalTransactions-" + totalTransactions)


    val totalsByClient = df.groupBy("clientIndex")
      .agg(
        bround(avg(col("prodsCost") / col("prodUds")), 2).as("avgSpentPerProd"),
        bround(sum("prodUds"), 2).as("amountOfProds"),
        countDistinct("prodName").as("variety"),
        bround(sum("prodsCost"), 2).as("spent"),
        countDistinct("transac").as("transacOfClient")
      )
    utilities.printFile(totalsByClient, resultsDir, appName + "_totalsByClient")


    val gastoPorCliente = totalsByClient
      .select("clientIndex", "spent")
      .withColumn("percentSpent", bround(col("spent") * 100 / totalEarns, 2))
      .orderBy(desc("spent"))
    utilities.printFile(gastoPorCliente, resultsDir, appName + "_clientsWhoSpentMore")

    val productosPorCliente = totalsByClient
      .select("clientIndex", "amountOfProds")
      .withColumn("percentAmountOfProds", bround(col("amountOfProds") * 100 / totalEarns, 2))
      .orderBy(desc("amountOfProds"))
    utilities.printFile(productosPorCliente, resultsDir, appName + "_clientsWhoBuyMoreProds")

    val clientesHabituales = totalsByClient
      .select("clientIndex", "transacOfClient")
      .withColumn("percentTransacOfClient", bround(col("transacOfClient") * 100 / totalTransactions, 2))
      .orderBy(desc("transacOfClient"))
    utilities.printFile(clientesHabituales, resultsDir, appName + "_clientswithMostTransactions")

    val gastoPorCompra = totalsByClient
      .groupBy("clientIndex")
      .agg(bround(avg(col("spent") / col("transacOfClient")), 2).as("avgSpentPerTransac"))
      .orderBy(desc("avgSpentPerTransac"))
    utilities.printFile(gastoPorCompra, resultsDir, appName + "_clientsSpentMorePerTransaction")

    val productosPorCompra = totalsByClient
        .groupBy("clientIndex")
      .agg(bround(avg(col("amountOfProds") / col("transacOfClient")), 2).as("avgProdsPerTransac"))
      .orderBy(desc("avgProdsPerTransac"))
    utilities.printFile(productosPorCompra, resultsDir, appName + "_clientsMoreProdsPerTransaction")

    val clientesComprasVariadas = totalsByClient
      .select("clientIndex", "variety")
      .withColumn("percentVariety", bround(col("variety") * 100 / totalDifferentItems, 2))
      .orderBy(desc("variety"))
    utilities.printFile(clientesComprasVariadas, resultsDir, appName + "_clientswithMostProductVariety")
  }
}

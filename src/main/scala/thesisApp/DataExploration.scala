package thesisApp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

class DataExploration {

  def dataExploration(df: DataFrame, resultsDir: String, utilities: UtilsCarrefourDataset, outputMode: String): Unit = {
    val analysisType = "dataExploration"

    val totals = df
      .agg(
        countDistinct("clientIndex").as("total_clients"),
        countDistinct("prodNameIndex").as("otal_products"),
        sum("prodUds").as("total_products_sold"),
        sum("prodsCost").as("total_earnings"),
        countDistinct("transac").as("total_transactions")
      )
    val Row(totalClients, totalProducts, totalItems, totalEarns, totalTransactions) = totals.collect()(0)

    val totalsByProd = df.groupBy("prodName")
      .agg(
        bround(avg(col("prodsCost") / col("prodUds")), 2).as("avgPrice"),
        bround(sum("prodUds")).as("udsSold"),
        bround(sum("prodUds") * 100 / totalItems, 2)as("percentUdsSold"),
        bround(sum("prodsCost"), 2).as("earnings"),
        bround(sum("prodsCost") * 100 / totalItems, 2)as("percentEarning"),
        countDistinct("transac").as("transacWithProd"),
        bround(sum("transac") * 100 / totalItems, 2)as("percentTransacWithProd")
      )

    utilities.printFile(totalsByProd, resultsDir, analysisType + "_totals", outputMode)
    utilities.printFile(totalsByProd, resultsDir, analysisType + "_totalsByProd", outputMode)


    val prodMasVendidos = totalsByProd
      .select("prodName", "udsSold", "percentUdsSold")
      .orderBy(desc("udsSold"))
    utilities.printFile(prodMasVendidos, resultsDir, analysisType + "_prodsMostSold_totalUds-" + totalItems, outputMode)

    val prodMasGanancias = totalsByProd
      .select("prodName", "earnings", "percentEarning")
      .orderBy(desc("earnings"))
    utilities.printFile(prodMasGanancias, resultsDir, analysisType + "_prodsMostEarns_totalEarns-" + totalEarns, outputMode)

    val prodsMostPopular = totalsByProd
      .select("prodName", "transacWithProd", "percentTransacWithProd")
      .orderBy(desc("transacWithProd"))
    utilities.printFile(prodsMostPopular, resultsDir, analysisType + "_prodsMostPopular_totalTransactions-" + totalTransactions, outputMode)
  }
}

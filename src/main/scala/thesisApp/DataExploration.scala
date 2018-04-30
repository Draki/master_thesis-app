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
    val Row(totalItems, totalEarns, totalTransactions) = totals
      .select("total_products_sold", "total_earnings", "total_transactions").collect()(0)

    val totalsByProd = df.groupBy("prodName")
      .agg(
        bround(avg(col("prodsCost") / col("prodUds")), 2).as("avgPrice"),
        bround(sum("prodUds")).as("udsSold"),
        bround(sum("prodUds") * 100 / totalItems, 2)as("percentUdsSold"),
        bround(sum("prodsCost"), 2).as("earnings"),
        bround(sum("prodsCost") * 100 / totalEarns, 2)as("percentEarning"),
        countDistinct("transac").as("transacWithProd"),
        bround(sum("transac") * 100 / totalTransactions, 2)as("percentTransacWithProd")
      )

    utilities.printFile(totals, resultsDir, appName + "_totals")
    utilities.printFile(totalsByProd, resultsDir, appName + "_totalsByProd")


    val prodMasVendidos = totalsByProd
      .select("prodName", "udsSold", "percentUdsSold")
      .orderBy(desc("udsSold"))
    utilities.printFile(prodMasVendidos, resultsDir, appName + "_prodsMostSold_totalUds-" + totalItems)

    val prodMasGanancias = totalsByProd
      .select("prodName", "earnings", "percentEarning")
      .orderBy(desc("earnings"))
    utilities.printFile(prodMasGanancias, resultsDir, appName + "_prodsMostEarns_totalEarns-" + totalEarns)

    val prodsMostPopular = totalsByProd
      .select("prodName", "transacWithProd", "percentTransacWithProd")
      .orderBy(desc("transacWithProd"))
    utilities.printFile(prodsMostPopular, resultsDir, appName + "_prodsMostPopular_totalTransactions-" + totalTransactions)
  }
}

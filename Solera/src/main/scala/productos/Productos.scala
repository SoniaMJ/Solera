package productos

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Productos{

  import org.apache.spark.SparkContext

  def main(args: Array[String]) {

    lazy val spark:SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName).getOrCreate()

    val productSchema =
      StructType(
        StructField("productId", IntegerType, false) ::
          StructField("description", StringType, false) ::
          StructField("price", FloatType, false) ::
          StructField("stock", IntegerType, false) ::
          Nil)


    val transactionsSchema =
      StructType(
        StructField("date", StringType, false) ::
          StructField("time", StringType, false) ::
          StructField("clientId", IntegerType, false) ::
          StructField("productId", IntegerType, false) ::
          StructField("number_of_items", IntegerType, false) ::
          StructField("total_amount_money", FloatType, false) ::
          Nil)

    val productsDF = spark.read.format("csv")
      .schema(productSchema)
      .option("header","false")
      .option("delimiter","#")
      .load("src/main/resources/productos/products.txt")

    val transactionsDF  = spark.read.format("csv")
      .schema(transactionsSchema)
      .option("header","false")
      .option("delimiter","#")
      .load("src/main/resources/productos/transactions.txt")

    import spark.implicits._


    //1 Print the client that has spent more money: client id and total amount of money.
	
      val biggestClient = transactionsDF.groupBy("clientId")
      .sum("total_amount_money")
      .withColumnRenamed("sum(total_amount_money)", "total_amount_money")
      .sort($"total_amount_money".desc)
      .take(1)(0)


    println("Ejercicio 1")
    println(s"client with id: ${biggestClient.get(0)} has expenses with amount:  ${biggestClient.get(1)}")



//   2  Write to an orc file the summary of all sold products (id, description, items sold,
//      total amount) with the corresponding number_of_items sold and the total amount of
//      money.



    val productsSoldDF = transactionsDF
      .groupBy("productId")
      .sum("total_amount_money", "number_of_items")
      .join(productsDF, "productId")
      .drop("price", "stock")


//    productsSoldDF.write.orc("src/main/resources/productsOutput/productsSold")


//   3 Write to an orc file the list of products (id, description, price) that have never been
//    sold.


    productsDF.createOrReplaceTempView("products")
    productsDF.createOrReplaceTempView("transactions")
    val unsoldProducts = spark.sql("select p.productId, p.description, p.price from products p left outer join transactions t on p.productId == t.productId where t.productId is NULL")


    unsoldProducts.write.orc("src/main/resources/productsOutput/unsoldProducts")


//   4 Write to a Parquet file the transactions corresponding to the 10 products with more
//    stock partitioned by year, month, day, hour.


    val topStockedProducts = spark.createDataFrame(sc.parallelize(productsDF.sort($"stock".desc).take(10)), productSchema)


    val topStockedTransactions = transactionsDF
      .join(topStockedProducts, "productId")
      .withColumn("_tmp", split($"date", "\\-"))
      .withColumn("year", $"_tmp".getItem(0))
      .withColumn("month", $"_tmp".getItem(1))
      .withColumn("day", $"_tmp".getItem(2))
      .drop("_tmp")


    topStockedTransactions.write.partitionBy("year", "month", "day", "time").parquet("src/main/resources/productsOutput/topStockedTransactions")
  }

}

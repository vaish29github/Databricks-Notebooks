// Databricks notebook source
// Declare an immutable list
val nums = List(10, 20, 30)
println(nums) // Output: List(10, 20, 30)

// Create a sequence of tuples and convert to DataFrame
val data = Seq((1, "Alice", "in wonderland"), (2, "Bob", "the builder"))
val df = spark.createDataFrame(data).toDF("id", "name", "series")
df.show()

// COMMAND ----------

val salesData = Seq(
  ("2024-01-01", "North", "Product A", 10, 200.0),
  ("2024-01-01", "South", "Product B", 5, 300.0),
  ("2024-01-02", "North", "Product A", 20, 400.0),
  ("2024-01-02", "South", "Product B", 10, 600.0),
  ("2024-01-03", "East",  "Product C", 15, 375.0)
)
val df = spark.createDataFrame(salesData).toDF("date", "region", "product", "quantity", "revenue")
df.show()

// COMMAND ----------

// DBTITLE 1,Total Revenue by Region
import org.apache.spark.sql.functions._

df.groupBy("region").agg(sum("revenue").alias("total_revenue")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## DATATYPES IN SCALA

// COMMAND ----------

// DBTITLE 1,Sum
val x = 5
val y = 10
println(x+y)

// COMMAND ----------

// DBTITLE 1,Print list and its sum
val list = List(1, 2, 3, 4, 5)  
println(list)  
println(list.sum)  

// COMMAND ----------

// DBTITLE 1,Calculate Average
val tup = (1, 2, 3, 4)  
println(tup)

val sum = tup.productIterator.map(_.asInstanceOf[Int]).sum  
println(sum)

// COMMAND ----------

val mySet = scala.collection.mutable.Set(1, 2, 3, 4, 5, 6)  
println(mySet)  
mySet.add(7)  
println(mySet)  
mySet ++= Set(11, 12, 13)  // Use ++= for adding multiple elements  
println(mySet)  

// COMMAND ----------

val dict = scala.collection.mutable.Map("name" -> "Atishay", "age" -> 21)  
println(dict("name"))  
dict("age") = 22  
println(dict.keys)  
println(dict.values)  

// COMMAND ----------

import org.apache.spark.sql.SparkSession  

val spark = SparkSession.builder.appName("ScalaDF").getOrCreate()  
// Creating DataFrame from list of tuples  
val data = Seq((1, "Alice"), (2, "Bob"), (3, "Carol"))  
val df = spark.createDataFrame(data).toDF("id", "name")  
df.show()  

// COMMAND ----------

df.select("name").show()  

// COMMAND ----------

df.filter($"id" > 1).show()  

// COMMAND ----------

println(df.count())  

// COMMAND ----------

df.describe().show()  

// COMMAND ----------

import org.apache.spark.sql.functions.lit  
val dfWithCountry = df.withColumn("country", lit("India"))  
dfWithCountry.show()   

// COMMAND ----------

val salesData = Seq(  
    ("2024-01-01", "North", "Product A", 10, 200.0),  
    ("2024-01-01", "South", "Product B", 5, 300.0),  
    ("2024-01-02", "North", "Product A", 20, 400.0),  
    ("2024-01-02", "South", "Product B", 10, 600.0),  
    ("2024-01-03", "East",  "Product C", 15, 375.0)  
)  
val columns = Seq("date", "region", "product", "quantity", "revenue")  
val salesDf = spark.createDataFrame(salesData).toDF(columns: _*)  
salesDf.show()  

// COMMAND ----------

val totalRevenuePerProduct = salesDf.groupBy("product")  
  .agg(sum("revenue").alias("total_revenue"))  
totalRevenuePerProduct.show()

// COMMAND ----------

val totalQtyRegion = salesDf.groupBy("region")  
  .agg(sum("quantity").alias("by region"))  
totalQtyRegion.show()  

// COMMAND ----------

val avgRevenuePerProduct = salesDf.groupBy("product")  
  .agg(avg("revenue").alias("Avg_revenue_per_product"))  
avgRevenuePerProduct.show()  

// COMMAND ----------

val highestRevenue = salesDf.groupBy("region")  
  .agg(sum("revenue").alias("by region"))  
  .orderBy($"by region".desc)  
  .limit(1)  
highestRevenue.show() 

// COMMAND ----------

import org.apache.spark.sql.SparkSession  
import org.apache.spark.sql.functions._  

val spark = SparkSession.builder.appName("SalesData").getOrCreate()  

val salesData = Seq(  
  ("2024-01-01", "North", "Product A", 10, 200.0),  
  ("2024-01-01", "South", "Product B", 5, 300.0),  
  ("2024-01-02", "North", "Product A", 20, 400.0),  
  ("2024-01-02", "South", "Product B", 10, 600.0),  
  ("2024-01-03", "East",  "Product C", 15, 375.0)  
)  

val columns = Seq("date", "region", "product", "quantity", "revenue")  
val salesDf = spark.createDataFrame(salesData).toDF(columns: _*)  

// Group by product and sum the revenue  
val resultDf = salesDf.groupBy("product").agg(sum("revenue").alias("total_revenue"))  

// Display the result  
resultDf.show()  

// COMMAND ----------

val resultDf = salesDf.groupBy("region").agg(avg("revenue").alias("avg_per_region"))  
resultDf.show()  
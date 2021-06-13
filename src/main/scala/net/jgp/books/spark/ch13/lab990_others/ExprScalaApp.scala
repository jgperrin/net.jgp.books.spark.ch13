package net.jgp.books.spark.ch13.lab990_others

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, RowFactory, SparkSession, functions => F}

/**
 * Use of expr().
 *
 * @author rambabu.posa
 */
object ExprScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("expr()")
      .master("local[*]")
      .getOrCreate

    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("title", DataTypes.StringType, false),
      DataTypes.createStructField("start", DataTypes.IntegerType, false),
      DataTypes.createStructField("end", DataTypes.IntegerType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("bla", int2Integer(10), int2Integer(30)))

    var df = spark.createDataFrame(rows, schema)
    df.show()

    df = df.withColumn("time_spent", F.expr("end - start"))
      .drop("start")
      .drop("end")

    df.show()

    spark.stop
  }

}

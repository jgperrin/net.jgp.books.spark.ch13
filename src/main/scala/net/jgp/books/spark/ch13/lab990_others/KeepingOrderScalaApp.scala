package net.jgp.books.spark.ch13.lab990_others

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession, functions => F}

/**
 * Keeping the order of rows during transformations.
 *
 * @author rambabu.posa
 */
object KeepingOrderScalaApp {

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
      .appName("Splitting a dataframe to collect it")
      .master("local[*]")
      .getOrCreate

    var df: Dataset[Row] = createDataframe(spark)
    df.show()

    df = df.withColumn("__idx", F.monotonically_increasing_id)
    df.show()

    df = df.dropDuplicates("col2")
      .orderBy("__idx")
      .drop("__idx")

    df.show()

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("col1", DataTypes.IntegerType, false),
      DataTypes.createStructField("col2", DataTypes.StringType, false),
      DataTypes.createStructField("sum", DataTypes.DoubleType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create(int2Integer(1), "a", double2Double(3555204326.27)))
    rows.add(RowFactory.create(int2Integer(4), "b", double2Double(22273491.72)))
    rows.add(RowFactory.create(int2Integer(5), "c", double2Double(219175.0)))
    rows.add(RowFactory.create(int2Integer(3), "a", double2Double(219175.0)))
    rows.add(RowFactory.create(int2Integer(2), "c", double2Double(75341433.37)))
    spark.createDataFrame(rows, schema)
  }

}

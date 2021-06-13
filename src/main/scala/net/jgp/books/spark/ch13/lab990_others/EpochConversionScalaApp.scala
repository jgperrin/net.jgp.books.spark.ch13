package net.jgp.books.spark.ch13.lab990_others

import java.util.{ArrayList, Random}

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{Row, RowFactory, SparkSession, functions => F}

/**
 * Use of from_unixtime().
 *
 * @author rambabu.posa
 */
object EpochConversionScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   * @throws InterruptedException
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("from_unixtime()")
      .master("local[*]")
      .getOrCreate

    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("event", DataTypes.IntegerType, false),
      DataTypes.createStructField("ts", DataTypes.StringType, false)))

    // Building a df with a sequence of chronological timestamps
    val rows = new ArrayList[Row]
    var now:Long = System.currentTimeMillis / 1000
    for (i <- 0 until 1000) {
      rows.add(RowFactory.create(int2Integer(i), String.valueOf(now)))
      now += new Random().nextInt(3) + 1
    }

    var df = spark.createDataFrame(rows, schema)
    df.show()

    // Turning the timestamps to dates
    df = df.withColumn("date", F.from_unixtime(F.col("ts")))
    df.show()

    // Collecting the result and printing out
    val timeRows = df.collectAsList
    import scala.collection.JavaConversions._
    for (r <- timeRows) {
      printf("[%d] : %s (%s)\n", r.getInt(0), r.getString(1), r.getString(2))
    }

    spark.stop
  }

}

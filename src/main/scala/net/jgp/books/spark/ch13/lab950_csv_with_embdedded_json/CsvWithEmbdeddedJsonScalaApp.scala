package net.jgp.books.spark.ch13.lab950_csv_with_embdedded_json

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{Encoders, Row, SparkSession, functions => F}

/**
 * Ingesting a CSV with embedded JSON.
 *
 * @author rambabu.posa
 */
@SerialVersionUID(19711L)
class CsvWithEmbdeddedJsonScalaApp extends Serializable {

  /**
   * Turns a Row into JSON. Not very fail safe, but done to illustrate.
   *
   * @author rambabu.posa
   */
  @SerialVersionUID(19712L)
  final private class Jsonifier extends MapFunction[Row, String] {
    @throws[Exception]
    override def call(r: Row): String = {
      println(r.mkString)
      val sb = new StringBuffer
      sb.append("{ \"dept\": \"")
      sb.append(r.getString(0))
      sb.append("\",")
      var s = r.getString(1).toString
      if (s != null) {
        s = s.trim
        if (s.charAt(0) == '{')
          s = s.substring(1, s.length - 1)
      }
      sb.append(s)
      sb.append(", \"location\": \"")
      sb.append(r.getString(2))
      sb.append("\"}")
      sb.toString
    }
  }

  /**
   * The processing code.
   */
  def start(): Unit = {
    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Processing of invoices")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv("data/misc/csv_with_embedded_json.csv")

    df.show(5, false)
    df.printSchema()

    val ds = df.map(new Jsonifier, Encoders.STRING)
    ds.show(5, false)
    ds.printSchema()

    var dfJson = spark.read.json(ds)
    dfJson.show(5, false)
    dfJson.printSchema()

    dfJson = dfJson
      .withColumn("emp", F.explode(F.col("employee")))
      .drop("employee")

    dfJson.show(5, false)
    dfJson.printSchema()

    dfJson = dfJson
      .withColumn("emp_name",
        F.concat(F.col("emp.name.firstName"), F.lit(" "), F.col("emp.name.lastName")))
      .withColumn("emp_address",
        F.concat(F.col("emp.address.street"), F.lit(" "), F.col("emp.address.unit")))
      .withColumn("emp_city", F.col("emp.address.city"))
      .drop("emp")

    dfJson.show(5, false)
    dfJson.printSchema()

    spark.stop
  }

}

object CsvWithEmbdeddedJsonScalaApplication {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val app = new CsvWithEmbdeddedJsonScalaApp
    app.start

  }

}

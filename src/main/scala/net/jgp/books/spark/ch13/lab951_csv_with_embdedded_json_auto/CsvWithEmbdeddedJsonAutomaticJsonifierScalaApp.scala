package net.jgp.books.spark.ch13.lab951_csv_with_embdedded_json_auto

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Encoders, Row, SparkSession, functions => F}

/**
 * Ingesting a CSV with embedded JSON.
 *
 * @author rambabu.posa
 */
@SerialVersionUID(19713L)
class CsvWithEmbdeddedJsonAutomaticJsonifierScalaApp extends Serializable {

  /**
   * Turns a Row into JSON.
   *
   * @author rambabu.posa
   */
  @SerialVersionUID(19714L)
  final private class Jsonifier(val jsonColumns: String*) extends MapFunction[Row, String] {
    private var fields: Array[StructField] = null

    @throws[Exception]
    override def call(r: Row): String = {
      if (fields == null)
        fields = r.schema.fields
      val sb: StringBuffer = new StringBuffer
      sb.append('{')
      var fieldIndex: Int = -1
      var isJsonColumn: Boolean = false
      for (f <- fields) {
        isJsonColumn = false
        fieldIndex += 1
        if (fieldIndex > 0) sb.append(',')
        if (jsonColumns.contains(f.name)) isJsonColumn = true
        if (!isJsonColumn) {
          sb.append('"')
          sb.append(f.name)
          sb.append("\": ")
        }
        val `type`: String = f.dataType.toString
        `type` match {
          case "IntegerType" =>
            sb.append(r.getInt(fieldIndex))

          case "LongType" =>
            sb.append(r.getLong(fieldIndex))

          case "DoubleType" =>
            sb.append(r.getDouble(fieldIndex))

          case "FloatType" =>
            sb.append(r.getFloat(fieldIndex))

          case "ShortType" =>
            sb.append(r.getShort(fieldIndex))

          case _ =>
            if (isJsonColumn) { // JSON field
              var s: String = r.getString(fieldIndex)
              if (s != null) {
                s = s.trim
                if (s.charAt(0) == '{') s = s.substring(1, s.length - 1)
              }
              sb.append(s)
            }
            else {
              sb.append('"')
              sb.append(r.getString(fieldIndex))
              sb.append('"')
            }

        }
      }
      sb.append('}')
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
      .option("header", true)
      .option("delimiter", "|")
      .option("inferSchema", true)
      .csv("data/misc/csv_with_embedded_json2.csv")

    df.show(5, false)
    df.printSchema()

    val ds = df.map(new Jsonifier("emp_json"), Encoders.STRING)
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

object CsvWithEmbdeddedJsonAutomaticJsonifierScalaApplication {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val app = new CsvWithEmbdeddedJsonAutomaticJsonifierScalaApp
    app.start

  }

}

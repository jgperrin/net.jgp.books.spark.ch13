package net.jgp.books.spark.ch13.lab900_flat_json_invoice


import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

/**
 * Automatic flattening of JSON structure in Spark.
 *
 * @author rambabu.posa
 */
object FlattenJsonScalaApp {
  val ARRAY_TYPE = "Array"
  val STRUCT_TYPE = "Struc"

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
      .appName("Automatic flattening of a JSON document")
      .master("local[*]")
      .getOrCreate

    // Reads a JSON, stores it in a dataframe
    val invoicesDf = spark.read
      .format("json")
      .option("multiline", true)
      .load("data/json/nested_array.json")

    // Shows at most 3 rows from the dataframe
    invoicesDf.show(3)
    invoicesDf.printSchema()

    val flatInvoicesDf = flattenNestedStructure(spark, invoicesDf)
    flatInvoicesDf.show(20, false)
    flatInvoicesDf.printSchema()

    spark.stop
  }

  /**
   * Implemented as a public static so you can copy it easily in your utils
   * library.
   *
   * @param spark
   * @param df
   * @return
   */
  def flattenNestedStructure(spark: SparkSession, df: DataFrame): DataFrame = {
    var recursion = false
    var processedDf = df
    val schema = df.schema
    val fields = schema.fields

    for (field <- fields) {
      field.dataType.toString.substring(0, 5) match {
        case ARRAY_TYPE =>
          // Explodes array
          processedDf = processedDf.withColumnRenamed(field.name, field.name + "_tmp")
          processedDf = processedDf.withColumn(field.name, F.explode(F.col(field.name + "_tmp")))
          processedDf = processedDf.drop(field.name + "_tmp")
          recursion = true

        case STRUCT_TYPE =>
          // Mapping
          /**
           * field.toDDL = `author` STRUCT<`city`: STRING, `country`: STRING, `name`: STRING, `state`: STRING>
           * field.toDDL = `publisher` STRUCT<`city`: STRING, `country`: STRING, `name`: STRING, `state`: STRING>
           * field.toDDL = `books` STRUCT<`salesByMonth`: ARRAY<BIGINT>, `title`: STRING>
           */
          println(s"field.toDDL = ${field.toDDL}")
          val ddl = field.toDDL.split("`") // fragile :(
          var i = 3
          while (i < ddl.length) {
            processedDf = processedDf.withColumn(field.name + "_" + ddl(i), F.col(field.name + "." + ddl(i)))
            i += 2
          }
          processedDf = processedDf.drop(field.name)
          recursion = true
        case _ =>
          processedDf = processedDf
      }
    }

    if (recursion)
      processedDf = flattenNestedStructure(spark, processedDf)

    processedDf
  }

}

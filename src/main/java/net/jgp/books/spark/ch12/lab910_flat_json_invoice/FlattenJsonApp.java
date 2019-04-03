package net.jgp.books.spark.ch12.lab910_flat_json_invoice;

import static org.apache.spark.sql.functions.explode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Automatic flattening of JSON structure in Spark.
 * 
 * @author jgp
 */
public class FlattenJsonApp {
  public static final String ARRAY_TYPE = "Array";
  public static final String STRUCT_TYPE = "Struc";

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    FlattenJsonApp app =
        new FlattenJsonApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Automatic flattening of a JSON document")
        .master("local")
        .getOrCreate();

    // Reads a JSON, stores it in a dataframe
    Dataset<Row> invoicesDf = spark.read()
        .format("json")
        .option("multiline", true)
        .load("data/json/nested_array.json");

    // Shows at most 3 rows from the dataframe
    invoicesDf.show(3);
    invoicesDf.printSchema();

    Dataset<Row> flatInvoicesDf = flattenNestedStructure(spark, invoicesDf);
    flatInvoicesDf.show(20, false);
    flatInvoicesDf.printSchema();
  }

  /**
   * Implemented as a public static so you can copy it easily in your utils
   * library.
   * 
   * @param spark
   * @param df
   * @return
   */
  public static Dataset<Row> flattenNestedStructure(
      SparkSession spark,
      Dataset<Row> df) {
    boolean recursion = false;

    Dataset<Row> processedDf = df;
    StructType schema = df.schema();
    StructField fields[] = schema.fields();
    for (StructField field : fields) {
      switch (field.dataType().toString().substring(0, 5)) {
        case ARRAY_TYPE:
          // Explodes array
          processedDf = processedDf
              .withColumnRenamed(field.name(), field.name() + "_tmp");
          processedDf = processedDf.withColumn(
              field.name(),
              explode(processedDf.col(field.name() + "_tmp")));
          processedDf = processedDf.drop(field.name() + "_tmp");
          recursion = true;
          break;
        case STRUCT_TYPE:
          // Mapping
          String ddl[] = field.toDDL().split("`"); // fragile :(
          for (int i = 3; i < ddl.length; i += 2) {
            processedDf = processedDf.withColumn(
                field.name() + "_" + ddl[i],
                processedDf.col(field.name() + "." + ddl[i]));
          }
          processedDf = processedDf
              .drop(field.name());
          recursion = true;
          break;
      }
    }
    if (recursion) {
      processedDf = flattenNestedStructure(spark, processedDf);
    }
    return processedDf;
  }
}

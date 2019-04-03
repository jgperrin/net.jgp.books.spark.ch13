package net.jgp.books.spark.ch12.lab950_csv_with_embdedded_json;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Ingesting a CSV with embedded JSON.
 * 
 * @author jgp
 */
public class CsvWithEmbdeddedJsonApp implements Serializable {
  private static final long serialVersionUID = 19711L;

  /**
   * Turns a Row into JSON. Not very fail safe, but done to illustrate.
   * 
   * @author jgp
   */
  private final class Jsonifier
      implements MapFunction<Row, String> {
    private static final long serialVersionUID = 19712L;

    @Override
    public String call(Row r) throws Exception {
      System.out.println(r.mkString());
      StringBuffer sb = new StringBuffer();
      sb.append("{ \"dept\": \"");
      sb.append(r.getString(0));
      sb.append("\",");
      String s = r.getString(1).toString();
      if (s != null) {
        s = s.trim();
        if (s.charAt(0) == '{') {
          s = s.substring(1, s.length() - 1);
        }
      }
      sb.append(s);
      sb.append(", \"location\": \"");
      sb.append(r.getString(2));
      sb.append("\"}");
      return sb.toString();
    }
  }

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    CsvWithEmbdeddedJsonApp app = new CsvWithEmbdeddedJsonApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Processing of invoices")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark
        .read()
        .option("header", "true")
        .option("delimiter", "|")
        .csv("data/misc/csv_with_embedded_json.csv");
    df.show(5, false);
    df.printSchema();

    Dataset<String> ds = df.map(
        new Jsonifier(),
        Encoders.STRING());
    ds.show(5, false);
    ds.printSchema();

    Dataset<Row> dfJson = spark.read().json(ds);
    dfJson.show(5, false);
    dfJson.printSchema();

    dfJson = dfJson
        .withColumn("emp", explode(dfJson.col("employee")))
        .drop("employee");
    dfJson.show(5, false);
    dfJson.printSchema();

    dfJson = dfJson
        .withColumn("emp_name",
            concat(
                dfJson.col("emp.name.firstName"),
                lit(" "),
                dfJson.col("emp.name.lastName")))
        .withColumn("emp_address",
            concat(dfJson.col("emp.address.street"),
                lit(" "),
                dfJson.col("emp.address.unit")))
        .withColumn("emp_city", dfJson.col("emp.address.city"))
        .drop("emp");
    dfJson.show(5, false);
    dfJson.printSchema();
  }
}

package net.jgp.books.spark.ch12.lab951_csv_with_embdedded_json_auto;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

/**
 * Ingesting a CSV with embedded JSON.
 * 
 * @author jgp
 */
public class CsvWithEmbdeddedJsonAutomaticJsonifierApp
    implements Serializable {
  private static final long serialVersionUID = 19713L;

  /**
   * Turns a Row into JSON.
   * 
   * @author jgp
   */
  private final class Jsonifier
      implements MapFunction<Row, String> {
    private static final long serialVersionUID = 19714L;

    private StructField[] fields = null;
    private List<String> jsonColumns = null;

    public Jsonifier(String... jsonColumns) {
      this.fields = null;
      this.jsonColumns = Arrays.asList(jsonColumns);
    }

    @Override
    public String call(Row r) throws Exception {
      if (fields == null) {
        fields = r.schema().fields();
      }
      StringBuffer sb = new StringBuffer();
      sb.append('{');
      int fieldIndex = -1;
      boolean isJsonColumn;
      for (StructField f : fields) {
        isJsonColumn = false;
        fieldIndex++;
        if (fieldIndex > 0) {
          sb.append(',');
        }
        if (this.jsonColumns.contains(f.name())) {
          isJsonColumn = true;
        }
        if (!isJsonColumn) {
          sb.append('"');
          sb.append(f.name());
          sb.append("\": ");
        }
        String type = f.dataType().toString();
        switch (type) {
          case "IntegerType":
            sb.append(r.getInt(fieldIndex));
            break;
          case "LongType":
            sb.append(r.getLong(fieldIndex));
            break;
          case "DoubleType":
            sb.append(r.getDouble(fieldIndex));
            break;
          case "FloatType":
            sb.append(r.getFloat(fieldIndex));
            break;
          case "ShortType":
            sb.append(r.getShort(fieldIndex));
            break;

          default:
            if (isJsonColumn) {
              // JSON field
              String s = r.getString(fieldIndex);
              if (s != null) {
                s = s.trim();
                if (s.charAt(0) == '{') {
                  s = s.substring(1, s.length() - 1);
                }
              }
              sb.append(s);
            } else {
              sb.append('"');
              sb.append(r.getString(fieldIndex));
              sb.append('"');
            }
            break;
        }
      }
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    CsvWithEmbdeddedJsonAutomaticJsonifierApp app =
        new CsvWithEmbdeddedJsonAutomaticJsonifierApp();
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
        .option("header", true)
        .option("delimiter", "|")
        .option("inferSchema", true)
        .csv("data/misc/csv_with_embedded_json2.csv");
    df.show(5, false);
    df.printSchema();

    Dataset<String> ds = df.map(
        new Jsonifier("emp_json"),
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

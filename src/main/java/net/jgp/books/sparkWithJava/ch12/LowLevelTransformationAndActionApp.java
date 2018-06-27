package net.jgp.books.sparkWithJava.ch12;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Low level transformations.
 * 
 * @author jgp
 */
public class LowLevelTransformationAndActionApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    LowLevelTransformationAndActionApp app =
        new LowLevelTransformationAndActionApp();
    app.start();
  }

  private final class ForeachFunctionExample implements ForeachFunction<Row> {
    private static final long serialVersionUID = 14738L;

    @Override
    public void call(Row t) throws Exception {
      // TODO Auto-generated method stub

    }
  }

  private final class BookUrlBuilder implements MapFunction<Row, String> {
    private static final long serialVersionUID = 26547L;

    @Override
    public String call(Row r) throws Exception {
      String s = "<a href='" + r.getString(4) + "'>" + r.getString(2) + "</a>";
      return s;
    }
  }

  /**
   * The processing code.
   */
  private void start() {

    SparkSession spark = SparkSession.builder()
        .appName("Low level transofrmation and actions")
        .master("local")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .load(
            "data/PEP_2017_PEPANNRES.csv");

    // Transformation
    //Dataset<String> dfString = df.map(new BookUrlBuilder(), Encoders.STRING());
    
    // Action
    //df.foreach(new ForeachFunctionExample());
  }
}

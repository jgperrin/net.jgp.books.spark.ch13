package net.jgp.books.sparkInAction.ch12.lab910FlatJsonInvoice;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * This is in progress.
 * 
 * @author jgp
 */
public class FlattenJsonApp {
  public static final String ARRAY_TYPE = "Array";
  public static final String STRUCT_TYPE = "Struc";
  public static final String SOURCE = "source";
  public static final String AS = " as ";
  public static final String DOT = ".";
  public static final String COMMA = ",";

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
        .load("data/json/one_record.json");

    // Shows at most 3 rows from the dataframe
    invoicesDf.show(3);
    invoicesDf.printSchema();

    Dataset<Row> flatInvoicesDf = flattenNestedStructure(invoicesDf, spark);
    flatInvoicesDf.show(3);
    flatInvoicesDf.printSchema();
  }

  private static Dataset<Row> flattenNestedStructure(
      Dataset<Row> nestedDataset,
      SparkSession spark) {

    // Lists all the columns in the Dataset with it's respective datatypes
    Tuple2<String, String>[] datatypes = nestedDataset.dtypes();
    Dataset<Row> commonJsonArrayStructure = null;
    Dataset<Row> commonJsonStructStructure = null;
    Dataset<Row> flattenDataForArray = null;
    Dataset<Row> flattenDataForStruct = null;
    List<String> fieldsOfTypeArray = Lists.newArrayList();
    List<String> dataTypesOfTypeStruct = Lists.newArrayList();

    // Adds list of 'ArrayType' and 'StructType' datatypes to
    // @link dataTypesOfTypeArray and @link dataTypesOfTypeStruct
    // respectively
    for (Tuple2<String, String> datatype : datatypes) {
      String extractedDatatype = datatype._2.substring(0, 5);
      switch (extractedDatatype) {
        case ARRAY_TYPE:
          fieldsOfTypeArray.add(datatype._1);
          break;

        case STRUCT_TYPE:
          dataTypesOfTypeStruct.add(datatype._1);
          break;

      }
    }

    // Map of Datatypes of type 'Array'
    for (String dataset : fieldsOfTypeArray) {
      commonJsonArrayStructure = nestedDataset.withColumn(dataset,
          org.apache.spark.sql.functions.explode(nestedDataset.col(
              dataset)));
      commonJsonArrayStructure.select(dataset);
      final String querySelectSQL = flattenSchema(commonJsonArrayStructure
          .schema(), null);
      flattenDataForArray = getFlattenedData(spark, querySelectSQL,
          commonJsonArrayStructure);
    }

    // Map of Datatypes of type 'Struct'
    for (String dataset : dataTypesOfTypeStruct) {
      commonJsonStructStructure = nestedDataset.select(nestedDataset.col(
          dataset));
      final String querySelectSQL = flattenSchema(commonJsonStructStructure
          .schema(), null);
      flattenDataForStruct = getFlattenedData(spark, querySelectSQL,
          commonJsonStructStructure);
    }

    return flattenDataForStruct;
  }

  private static Dataset<Row> getFlattenedData(SparkSession spark,
      String querySelectSQL,
      Dataset<Row> dataframeToExtractDataFrom) {
    dataframeToExtractDataFrom.createOrReplaceTempView(SOURCE);

    return spark.sql("SELECT " + querySelectSQL + " FROM source");
  }

  private static String flattenSchema(StructType schema, String prefix) {
    final StringBuilder selectSQLQuery = new StringBuilder();

    for (StructField field : schema.fields()) {
      final String fieldName = field.name();
      String colName;
      String prefixWithFieldName = prefix + DOT + fieldName;

      colName = prefix == null ? fieldName : prefixWithFieldName;

      // Final Column name
      String colNameTarget = fieldName;

      if (field.dataType().getClass().equals(StructType.class)) {
        selectSQLQuery.append(flattenSchema((StructType) field.dataType(),
            colName));
      } else {
        selectSQLQuery.append(colName);
        selectSQLQuery.append(AS);
        selectSQLQuery.append(colNameTarget);
      }

      selectSQLQuery.append(COMMA);
    }

    if (selectSQLQuery.length() > 0) {
      selectSQLQuery.deleteCharAt(selectSQLQuery.length() - 1);
    }

    return selectSQLQuery.toString();
  }

}

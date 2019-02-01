package net.jgp.books.sparkInAction.ch12.lab300Join;

import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Performs a join between 3 datasets.
 * 
 * @author jgp
 */
public class HigherEdInstitutionPerCountyApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    HigherEdInstitutionPerCountyApp app = new HigherEdInstitutionPerCountyApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creation of the session
    SparkSession spark = SparkSession.builder()
        .appName("Join")
        .master("local")
        .getOrCreate();

    // Ingestion of the census data
    Dataset<Row> censusDf = spark
        .read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "cp1252")
        .load("data/census/PEP_2017_PEPANNRES.csv");
    censusDf = censusDf
        .drop("GEO.id")
        .drop("rescen42010")
        .drop("resbase42010")
        .drop("respop72010")
        .drop("respop72011")
        .drop("respop72012")
        .drop("respop72013")
        .drop("respop72014")
        .drop("respop72015")
        .drop("respop72016")
        .withColumnRenamed("respop72017", "pop2017")
        .withColumnRenamed("GEO.id2", "countyId")
        .withColumnRenamed("GEO.display-label", "county");
    censusDf.sample(0.1).show(3, false);
    censusDf.printSchema();

    // Higher education institution (and yes, there is an Arkansas College
    // of Barbering and Hair Design)
    Dataset<Row> higherEdDf = spark
        .read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/dapip/InstitutionCampus.csv");
    higherEdDf = higherEdDf
        .filter("LocationType = 'Institution'")
        .withColumn("zip_tmp", split(higherEdDf.col("Address"), " "));
    higherEdDf = higherEdDf
        .withColumn("zip_tmp2", size(higherEdDf.col("zip_tmp")));
    higherEdDf = higherEdDf
        .withColumn(
            "zip_tmp3",
            element_at(
                higherEdDf.col("zip_tmp"),
                higherEdDf.col("zip_tmp2")));
    higherEdDf = higherEdDf
        .withColumn(
            "zip_tmp4",
            split(higherEdDf.col("zip_tmp3"), "-"));
    higherEdDf = higherEdDf
        .withColumn("zip", higherEdDf.col("zip_tmp4").getItem(0))
        .drop("DapipId")
        .drop("OpeId")
        .drop("ParentName")
        .drop("ParentDapipId")
        .drop("LocationType")
        .drop("Address")
        .drop("GeneralPhone")
        .drop("AdminName")
        .drop("AdminPhone")
        .drop("AdminEmail")
        .drop("Fax")
        .drop("UpdateDate")
        .drop("zip_tmp")
        .drop("zip_tmp2")
        .drop("zip_tmp3")
        .drop("zip_tmp4");
    higherEdDf.sample(0.1).show(3, false);
    higherEdDf.printSchema();

    // Zip to county
    Dataset<Row> countyZipDf = spark
        .read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/hud/COUNTY_ZIP_092018.csv");
    countyZipDf = countyZipDf
        .drop("res_ratio")
        .drop("bus_ratio")
        .drop("oth_ratio")
        .drop("tot_ratio");
    countyZipDf.sample(0.1).show(3, false);
    countyZipDf.printSchema();

    // Institution per county
    Dataset<Row> institutionPerCountyDf = higherEdDf.join(
        countyZipDf,
        higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
        "left_semi");
//    institutionPerCountyDf = institutionPerCountyDf.join(
//        censusDf,
//        institutionPerCountyDf.col("county")
//            .equalTo(censusDf.col("countyId")),
//        "left");
    //institutionPerCountyDf.sample(false, 0.9).show(20, false);
    institutionPerCountyDf.show(20, false);
    institutionPerCountyDf.printSchema();

//    institutionPerCountyDf = institutionPerCountyDf
//        .drop(countyZipDf.col("zip"))
//        .drop(countyZipDf.col("county"))
//        .drop("countyId");
//    institutionPerCountyDf.sample(0.9).show(10, false);
//    institutionPerCountyDf.printSchema();

    // A little more
    // @formatter:off
    //    Dataset<Row> aggDf = institutionPerCountyDf
    //        .groupBy("county", "pop2017")
    //        .count();
    //    aggDf = aggDf.orderBy(aggDf.col("count").desc());
    //    aggDf.show(25, false);
    //
    //    Dataset<Row> popDf = aggDf
    //        .filter("pop2017>30000")
    //        .withColumn("institutionPer10k", expr("count*10000/pop2017"));
    //    popDf = popDf.orderBy(popDf.col("institutionPer10k").desc());
    //    popDf.show(25, false);
    // @formatter:on
  }
}

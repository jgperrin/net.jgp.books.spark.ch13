The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).
  
# Spark in Action, 2nd edition – Java, Python, and Scala code for chapter 13

Chapter 13 introduces **transforming entire documents**.

This code is designed to work with Apache Spark v3.0.0.

Datasets can be downloaded from:
* ... (to be added)

## Lab

Each chapter has one or more labs. Labs are examples used for teaching in the book(https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**. This chapter has only one lab.

### Lab \#100

The `JsonShipmentDisplayApp` application does the following:

1.	It acquires a session (a `SparkSession`).
2.	It asks Spark to load (ingest) a dataset in JSON format.
3.	Spark stores the contents in a dataframe, then process of invoices formatted using the schema.org format.

## Running the lab in Java

For information on running the Java lab, see chapter 1 in [Spark in Action, 2nd edition](http://jgp.net/sia).

## Running the lab using PySpark

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch13
```

2. Go to the lab in the Python directory

```
cd net.jgp.books.spark.ch01/src/main/python/lab100_json_shipment/
```

3. Execute the following spark-submit command to create a jar file to our this application

 ```
spark-submit jsonShipmentDisplayApp.py
 ```

## Running the lab in Scala


Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips'). 

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch13
```

2. cd net.jgp.books.spark.ch01

3. Package application using sbt command

```
sbt clean assembly
```

4. Run Spark/Scala application using spark-submit command as shown below:

```
spark-submit --class net.jgp.books.spark.ch13.lab100_json_shipment.JsonShipmentDisplayScalaApp target/scala-2.12/SparkInAction2-Chapter13-assembly-1.0.0.jar
```



---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://fb.com/SparkInAction/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).

[1]: https://data.cityofnewyork.us
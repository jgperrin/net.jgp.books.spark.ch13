package net.jgp.books.spark.ch13.lab300_udaf;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QualityControlAggregationFunction
    extends UserDefinedAggregateFunction {
  private static Logger log =
      LoggerFactory.getLogger(QualityControlAggregationFunction.class);

  private static final long serialVersionUID = -66830400L;

  /**
   * Describes the schema of input sent to the UDAF. Spark UDAFs can operate
   * on any number of columns. In our use case, we only need one field.
   */
  public StructType inputSchema() {
    List<StructField> inputFields = new ArrayList<>();
    inputFields.add(
        DataTypes.createStructField("c0", DataTypes.DoubleType, true));
    return DataTypes.createStructType(inputFields);
  }

  /**
   * Describes the schema of UDAF buffer.
   */
  public StructType bufferSchema() {
    List<StructField> bufferFields = new ArrayList<>();
    bufferFields.add(
        DataTypes.createStructField("sum", DataTypes.DoubleType, true));
    return DataTypes.createStructType(bufferFields);
  }

  /**
   * Datatype of the UDAF's output.
   */
  public DataType dataType() {
    return DataTypes.DoubleType;
  }

  /**
   * Describes whether the UDAF is deterministic or not.
   * 
   * Since, Spark executes by splitting data, it processes the chunks
   * separately and combining them. If the UDAF logic is such that the
   * result is independent of the order in which data is processed and
   * combined then the UDAF is deterministic.
   */
  public boolean deterministic() {
    return true;
  }

  /**
   * Initializes the buffer. This method can be called any number of times
   * of Spark during processing.
   */
  public void initialize(MutableAggregationBuffer buffer) {
    buffer.update(
        0, // column
        0.0); // value
    // You can repeat that for the number of columns you have in your buffer
  }

  public void update(MutableAggregationBuffer buffer, Row input) {
    log.debug("-> update(), in put row has {} args", input.length());
    if (input.isNullAt(0)) {
      log.debug("Value passed is null.");
      return;
    }
    double inputValue = input.getDouble(0);
    log.debug("value passed to update() is {}", inputValue);
    if (inputValue <= 0) {
      // throw new InvalidInputValuesException();
    }
    double bufferSum = buffer.getDouble(0);
    long bufferCount = buffer.getLong(1);

    buffer.update(0, bufferSum + (1 / inputValue));
    buffer.update(1, bufferCount + 1);
  }

  public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
    buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0));
    buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
  }

  @Override
  public Double evaluate(Row arg0) {
    return ((double) arg0.getLong(1)) / arg0.getDouble(0);
  }
}
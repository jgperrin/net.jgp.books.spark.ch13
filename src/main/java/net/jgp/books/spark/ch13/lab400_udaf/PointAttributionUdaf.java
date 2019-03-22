package net.jgp.books.spark.ch13.lab400_udaf;

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

/**
 * 
 * @author jgp
 */
public class PointAttributionUdaf
    extends UserDefinedAggregateFunction {
  private static Logger log =
      LoggerFactory.getLogger(PointAttributionUdaf.class);

  private static final long serialVersionUID = -66830400L;

  public static final int MAX_POINT_PER_ORDER = 3;

  /**
   * Describes the schema of input sent to the UDAF. Spark UDAFs can operate
   * on any number of columns. In our use case, we only need one field.
   */
  @Override
  public StructType inputSchema() {
    List<StructField> inputFields = new ArrayList<>();
    inputFields.add(
        DataTypes.createStructField("c0", DataTypes.IntegerType, true));
    return DataTypes.createStructType(inputFields);
  }

  /**
   * Describes the schema of UDAF buffer.
   */
  @Override
  public StructType bufferSchema() {
    List<StructField> bufferFields = new ArrayList<>();
    bufferFields.add(
        DataTypes.createStructField("sum", DataTypes.IntegerType, true));
    return DataTypes.createStructType(bufferFields);
  }

  /**
   * Datatype of the UDAF's output.
   */
  @Override
  public DataType dataType() {
    return DataTypes.IntegerType;
  }

  /**
   * Describes whether the UDAF is deterministic or not.
   * 
   * Since, Spark executes by splitting data, it processes the chunks
   * separately and combining them. If the UDAF logic is such that the
   * result is independent of the order in which data is processed and
   * combined then the UDAF is deterministic.
   */
  @Override
  public boolean deterministic() {
    return true;
  }

  /**
   * Initializes the buffer. This method can be called any number of times
   * of Spark during processing.
   */
  @Override
  public void initialize(MutableAggregationBuffer buffer) {
    log.trace("-> initialize() - buffer as {} row(s)", buffer.length());
    buffer.update(
        0, // column
        0); // value
    // You can repeat that for the number of columns you have in your buffer
  }

  /**
   * Updates the buffer with an input row. Validations on input should be
   * performed in this method.
   */
  @Override
  public void update(MutableAggregationBuffer buffer, Row input) {
    log.trace("-> update(), input row has {} args", input.length());
    if (input.isNullAt(0)) {
      log.trace("Value passed is null.");
      return;
    }
    log.trace("-> update({}, {})", buffer.getInt(0), input.getInt(0));

    // Apply your business rule, could be in an external function/service.
    int initialValue = buffer.getInt(0);
    int inputValue = input.getInt(0);
    int outputValue = 0;
    if (inputValue < MAX_POINT_PER_ORDER) {
      outputValue = inputValue;
    } else {
      outputValue = MAX_POINT_PER_ORDER;
    }
    outputValue += initialValue;
    
    log.trace(
        "Value passed to update() is {}, this will grant {} points",
        inputValue,
        outputValue);
    buffer.update(0, outputValue);
  }

  @Override
  public void merge(MutableAggregationBuffer buffer, Row row) {
    log.trace("-> merge({}, {})", buffer.getInt(0), row.getInt(0));
    buffer.update(0, buffer.getInt(0) + row.getInt(0));
  }

  @Override
  public Integer evaluate(Row row) {
    log.trace("-> evaluate({})", row.getInt(0));
    return row.getInt(0);
  }
}
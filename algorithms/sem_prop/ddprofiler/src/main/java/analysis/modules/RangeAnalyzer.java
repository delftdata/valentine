/**
 * @author Raul - raulcf@csail.mit.edu
 * @author Sibo Wang (edits)
 */
package analysis.modules;

import java.util.List;
import com.clearspring.analytics.stream.quantile.QDigest;
import analysis.FloatDataConsumer;
import analysis.IntegerDataConsumer;

public class RangeAnalyzer implements IntegerDataConsumer, FloatDataConsumer {

  private long totalRecords;
  private long max = Integer.MIN_VALUE;
  private long min = Integer.MAX_VALUE;
  private long totalSum;

  private float maxF = Float.MIN_VALUE;
  private float minF = Float.MAX_VALUE;
  private float totalSumF;

  private boolean quantileIsUsed = false;

  /*
   * calculate the std_deviation
   */
  private double squareSum;
  private float avg;
  private float stdDeviation;

  final private int QUANTILE_COMPRESSION_RATIO = 128;
  /*
   * provide estimator of quantile.
   * Let c be the number of distinct values in the stream
   * the relative error is O(log(c)/QUANTILE_COMPRESSION_RATIO)
   * We make a conservative assumption that c can be as large as 2^64.
   * Then, to provide good estimation, we will need to set
   * QUANTILE_COMPRESSION_RATIO
   * to 128 to reach a reasonable relative estimation.
   */
  private QDigest quantileEstimator = new QDigest(QUANTILE_COMPRESSION_RATIO);

  public long getQuantile(double p) {
    if (!quantileIsUsed) {
      return -1;
    }
    return quantileEstimator.getQuantile(p);
  }

  public Range getIntegerRange() {
    avg = (float)(totalSum * 1.0 / totalRecords);
    stdDeviation = (float)Math.sqrt(squareSum / totalRecords - avg * avg);
    long quantile75 = this.getQuantile(0.75);
    long quantile25 = this.getQuantile(0.25);
    long iqr = quantile75 - quantile25;
    long median = this.getQuantile(0.5);
    Range r = Range.makeIntegerRange(DataType.Type.INT, totalRecords, max, min,
                                     avg, stdDeviation, median, iqr);
    return r;
  }

  public Range getFloatRange() {
    avg = totalSumF / totalRecords;
    stdDeviation = (float)Math.sqrt(squareSum / totalRecords - avg * avg);
    long quantile75 = this.getQuantile(0.75);
    long quantile25 = this.getQuantile(0.25);
    long iqr = quantile75 - quantile25;
    long median = this.getQuantile(0.5);
    Range r = Range.makeFloatRange(DataType.Type.FLOAT, totalRecords, maxF,
                                   minF, avg, stdDeviation, median, iqr);
    return r;
  }

  @Override
  public boolean feedIntegerData(List<Long> records) {

    for (long value : records) {
      if (!quantileIsUsed) {
        quantileIsUsed = true;
      }
      totalRecords++;
      if (value > max)
        max = value;
      if (value < min)
        min = value;
      totalSum += value;
      squareSum += value * value;
      if (value < 0)
        value =
            value *
            -1; // transforming negative to positive will create false positives
      quantileEstimator.offer(value);
    }

    return true;
  }

  @Override
  public boolean feedFloatData(List<Float> records) {

    for (float value : records) {
      if (!quantileIsUsed) {
        quantileIsUsed = true;
      }
      totalRecords++;
      if (value > maxF)
        maxF = value;
      if (value < minF)
        minF = value;
      totalSumF += value;
      squareSum += value * value;
      if (value < 0)
        value =
            value *
            -1; // transforming negative to positive will create false positives
      int newValue = (int)value;
      quantileEstimator.offer(newValue);
    }
    return true;
  }
}

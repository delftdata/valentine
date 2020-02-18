/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.util.List;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import analysis.FloatDataConsumer;
import analysis.IntegerDataConsumer;
import analysis.TextualDataConsumer;

public class CardinalityAnalyzer
    implements IntegerDataConsumer, FloatDataConsumer, TextualDataConsumer {

  private long totalRecords;
  private ICardinality ic;

  public CardinalityAnalyzer() {
    // ic = new HyperLogLogPlus(4, 16);
    ic = new HyperLogLogPlus(18, 25);
  }

  public Cardinality getCardinality() {
    long uniqueElements = ic.cardinality();
    Cardinality c = new Cardinality(totalRecords, uniqueElements);
    return c;
  }

  @Override
  public boolean feedTextData(List<String> records) {
    for (String r : records) {
      totalRecords++;
      ic.offer(r);
    }

    return true;
  }

  @Override
  public boolean feedFloatData(List<Float> records) {

    for (float r : records) {
      totalRecords++;
      ic.offer(r);
    }

    return true;
  }

  @Override
  public boolean feedIntegerData(List<Long> records) {

    for (long r : records) {
      totalRecords++;
      ic.offer(r);
    }

    return true;
  }
}

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import analysis.modules.Cardinality;
import analysis.modules.CardinalityAnalyzer;
import analysis.modules.Range;
import analysis.modules.RangeAnalyzer;
import sources.deprecated.Attribute.AttributeType;

public class NumericalAnalyzer implements NumericalAnalysis {

  private List<DataConsumer> analyzers;
  private CardinalityAnalyzer ca;
  private RangeAnalyzer ra;

  private NumericalAnalyzer() {
    analyzers = new ArrayList<>();
    ca = new CardinalityAnalyzer();
    ra = new RangeAnalyzer();
    analyzers.add(ca);
    analyzers.add(ra);
  }

  public static NumericalAnalyzer makeAnalyzer() {
    return new NumericalAnalyzer();
  }

  @Override
  public boolean feedIntegerData(List<Long> records) {

    Iterator<DataConsumer> dcs = analyzers.iterator();
    while (dcs.hasNext()) {
      IntegerDataConsumer dc = (IntegerDataConsumer)dcs.next();
      dc.feedIntegerData(records);
    }

    return false;
  }

  @Override
  public boolean feedFloatData(List<Float> records) {

    Iterator<DataConsumer> dcs = analyzers.iterator();
    while (dcs.hasNext()) {
      FloatDataConsumer dc = (FloatDataConsumer)dcs.next();
      dc.feedFloatData(records);
    }

    return false;
  }

  @Override
  public DataProfile getProfile() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Cardinality getCardinality() {
    return ca.getCardinality();
  }

  @Override
  public Range getNumericalRange(AttributeType type) {
    if (type.equals(AttributeType.FLOAT)) {
      return ra.getFloatRange();
    } else if (type.equals(AttributeType.INT)) {
      return ra.getIntegerRange();
    }
    return null;
  }

  @Override
  public long getQuantile(double p) {
    return ra.getQuantile(p);
  }
}

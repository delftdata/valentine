package preanalysis;

import java.util.List;

import sources.deprecated.Attribute.AttributeType;

public class Values {

  final private AttributeType type;

  private List<Float> floats;
  private List<String> strings;
  private List<Long> integers;

  private Values(AttributeType type) { this.type = type; }

  private void setFloats(List<Float> values) { this.floats = values; }

  private void setStrings(List<String> values) { this.strings = values; }

  private void setIntegers(List<Long> values) { this.integers = values; }

  public static Values makeFloatValues(List<Float> castValues) {
    Values vs = new Values(AttributeType.FLOAT);
    vs.setFloats(castValues);
    return vs;
  }

  public static Values makeStringValues(List<String> castValues) {
    Values vs = new Values(AttributeType.STRING);
    vs.setStrings(castValues);
    return vs;
  }

  public static Values makeIntegerValues(List<Long> castValues) {
    Values vs = new Values(AttributeType.INT);
    vs.setIntegers(castValues);
    return vs;
  }

  public boolean areFloatValues() {
    return this.type.equals(AttributeType.FLOAT);
  }

  public boolean areIntegerValues() {
    return this.type.equals(AttributeType.INT);
  }

  public boolean areStringValues() {
    return this.type.equals(AttributeType.STRING);
  }

  public List<Float> getFloats() { return floats; }

  public List<String> getStrings() { return strings; }

  public List<Long> getIntegers() { return integers; }
}

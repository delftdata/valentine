package analysis;

import java.util.List;

public interface FloatDataConsumer extends DataConsumer {

  public boolean feedFloatData(List<Float> records);
}

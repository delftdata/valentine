package analysis;

import java.util.List;

public interface IntegerDataConsumer extends DataConsumer {

  public boolean feedIntegerData(List<Long> records);
}

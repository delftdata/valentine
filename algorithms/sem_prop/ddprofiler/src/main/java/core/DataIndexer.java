package core;

import java.util.Map;

import preanalysis.Values;
import sources.deprecated.Attribute;

public interface DataIndexer {

  public boolean indexData(String dbName, String path, Map<Attribute, Values> data);
  public boolean flushAndClose();
}

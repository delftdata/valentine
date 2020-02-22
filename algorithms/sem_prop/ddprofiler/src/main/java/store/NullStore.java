package store;

import java.util.List;

import core.WorkerTaskResult;

public class NullStore implements Store {

  @Override
  public void initStore() {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean indexData(long id, String dbName, String path, String sourceName, String columnName,
                           List<String> values) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean storeDocument(WorkerTaskResult wtr) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void tearDownStore() {
    // TODO Auto-generated method stub
  }
}

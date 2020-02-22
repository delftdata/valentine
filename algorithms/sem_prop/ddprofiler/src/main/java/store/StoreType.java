package store;

public enum StoreType {
  NULL(0),
  ELASTIC_HTTP(1),
  ELASTIC_NATIVE(2);

  private int type;

  StoreType(int type) { this.type = type; }

  public int ofType() { return type; }
}

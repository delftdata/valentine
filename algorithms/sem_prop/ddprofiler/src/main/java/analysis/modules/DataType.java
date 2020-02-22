/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

public class DataType {

  public enum Type {
    INT,
    DOUBLE,
    BYTE,
    STRING,
    CHAR,
    DATE,
    FLOAT;
  }

  private Type type;

  public Type getType() { return type; }
}

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

public class Cardinality {

  private long totalRecords;
  private long uniqueElements;

  public Cardinality(long totalRecords, long uniqueElements) {
    this.totalRecords = totalRecords;
    this.uniqueElements = uniqueElements;
  }

  public long getTotalRecords() { return totalRecords; }

  public long getUniqueElements() { return uniqueElements; }

  @Override
  public String toString() {
    return uniqueElements + "/" + totalRecords;
  }
}

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis;

import analysis.modules.Cardinality;
import analysis.modules.DataType;

public interface Analysis {

  public DataProfile getProfile();
  public Cardinality getCardinality();
}

package analysis;

import analysis.modules.Entities;

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
public interface TextualAnalysis extends Analysis, TextualDataConsumer {

  public Entities getEntities();
  public long[] getMH();
  
}

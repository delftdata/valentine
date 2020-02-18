/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Entities {

  private Set<String> entities;

  public Entities(Set<String> entities) { this.entities = entities; }

  public List<String> getEntities() {
    List<String> ents = new ArrayList<>();
    entities.forEach(e -> ents.add(e));
    return ents;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();

    sb.append("[");
    for (String s : entities) {
      sb.append(s);
      sb.append(" - ");
    }
    sb.append("]");

    return sb.toString();
  }
}

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package core.config;

import core.config.ConfigDef.Importance;
import core.config.ConfigDef.Type;
import core.config.ConfigDef.Validator;

public class ConfigKey {

  private static final Object NO_DEFAULT_VALUE = new String("");

  public final String name;
  public final Type type;
  public final String documentation;
  public final Object defaultValue;
  public final Validator validator;
  public final Importance importance;

  public ConfigKey(String name, Type type, Object defaultValue,
                   Validator validator, Importance importance,
                   String documentation) {
    super();
    this.name = name;
    this.type = type;
    this.defaultValue = defaultValue;
    this.validator = validator;
    this.importance = importance;
    if (this.validator != null)
      this.validator.ensureValid(name, defaultValue);
    this.documentation = documentation;
  }

  public String getName() { return this.name; }

  public boolean hasDefault() { return this.defaultValue != NO_DEFAULT_VALUE; }
}

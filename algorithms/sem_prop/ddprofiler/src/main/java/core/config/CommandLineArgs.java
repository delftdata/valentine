/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package core.config;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import core.config.ConfigDef.Type;

public class CommandLineArgs {

  private OptionSet options;

  public CommandLineArgs(String[] args, OptionParser parser,
                         List<ConfigKey> c) {
    configureParser(parser, c);
    options = parser.parse(args);
  }

  private void configureParser(OptionParser parser, List<ConfigKey> c) {
    for (ConfigKey key : c) {
      String name = key.name;
      String doc = key.documentation;
      core.config.ConfigDef.Type type = key.type;
      if (type == core.config.ConfigDef.Type.BOOLEAN) {
        parser.accepts(name, doc)
            .withRequiredArg()
            .ofType(Boolean.class)
            .defaultsTo((boolean)key.defaultValue);
      } else if (type == Type.DOUBLE) {
        parser.accepts(name, doc)
            .withRequiredArg()
            .ofType(Double.class)
            .defaultsTo((double)key.defaultValue);
      } else if (type == Type.INT) {
        parser.accepts(name, doc)
            .withRequiredArg()
            .ofType(Integer.class)
            .defaultsTo((int)key.defaultValue);
      } else if (type == Type.LONG) {
        parser.accepts(name, doc)
            .withRequiredArg()
            .ofType(Long.class)
            .defaultsTo((long)key.defaultValue);
      } else if (type == Type.STRING) {
        parser.accepts(name, doc)
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo((String)key.defaultValue);
      }
    }
    parser.accepts("help").forHelp();
  }

  public Properties getProperties() {
    return CommandLineArgs.asProperties(options);
  }

  private static Properties asProperties(OptionSet options) {
    Properties properties = new Properties();
    for (Entry<OptionSpec<?>, List<?>> entry : options.asMap().entrySet()) {
      OptionSpec<?> spec = entry.getKey();
      String key = asPropertyKey(spec);
      String value = asPropertyValue(entry.getValue(), options.has(spec));
      properties.setProperty(key, value);
    }
    return properties;
  }

  private static String asPropertyKey(OptionSpec<?> spec) {
    List<String> flags = (List<String>)spec.options();
    for (String flag : flags)
      if (1 < flag.length())
        return flag;
    throw new IllegalArgumentException("No usable non-short flag: " + flags);
  }

  private static String asPropertyValue(List<?> values, boolean present) {
    // Simple flags have no values; treat presence/absence as true/false
    String value = "";
    if (values.isEmpty()) {
      return String.valueOf(present);
    } else {
      for (int i = 0; i < values.size(); i++) {
        if (i != 0) {
          value.concat(",");
        }
        value = value.concat(String.valueOf(values.get(i)));
      }
    }
    return value;
  }

  /**
   * Gets the command line arguments that are for the query
   * (all the arguments that were not specified to the parser to accept)
   * @return array of query command line arguments
   */
  public String[] getQueryArgs() {
    return options.nonOptionArguments().toArray(new String[0]);
  }
}

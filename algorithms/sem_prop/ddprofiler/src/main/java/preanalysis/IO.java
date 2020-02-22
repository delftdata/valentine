/**
 * @author ra-mit
 */
package preanalysis;

import java.util.Map;

import sources.deprecated.Attribute;

public interface IO { 
	public Map<Attribute, Values> readRows(int num) throws Exception; 
}

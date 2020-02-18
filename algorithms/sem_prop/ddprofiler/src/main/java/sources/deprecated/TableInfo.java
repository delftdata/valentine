/**
 * @author Sibo Wang
 * @author ra-mit (edits)
 *
 */
package sources.deprecated;

import java.util.List;

@Deprecated
public class TableInfo {

    private List<Attribute> tableAttributes;

    public TableInfo() {
    }

    public List<Attribute> getTableAttributes() {
	return tableAttributes;
    }

    public void setTableAttributes(List<Attribute> tableAttributes) {
	this.tableAttributes = tableAttributes;
    }
}

/**
 * @author Sibo Wang
 *
 */
package sources.deprecated;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Deprecated
public class Record {

    private List<String> tuples;

    public Record() {
	tuples = new ArrayList<String>();
    }

    public void setTuples(List<String> tuples) {
	this.tuples = tuples;
    }

    public void setTuples(String[] res) {
	this.tuples.addAll(Arrays.asList(res));
    }

    public List<String> getTuples() {
	return this.tuples;
    }

    @Override
    public String toString() {
	String res = "Record(";
	for (int i = 0; i < tuples.size(); i++) {
	    res += "|" + tuples.get(i) + "|,";
	}
	res += ")";
	return res;
    }
}

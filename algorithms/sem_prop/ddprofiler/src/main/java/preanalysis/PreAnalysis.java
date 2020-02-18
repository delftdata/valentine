/**
 * @author ra-mit
 */
package preanalysis;

import java.util.List;

import sources.Source;
import sources.deprecated.Attribute;

public interface PreAnalysis {

    public void assignSourceTask(Source c);

    public DataQualityReport getQualityReport();

    public List<Attribute> getEstimatedDataTypes();
}

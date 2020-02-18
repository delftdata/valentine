package sources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import sources.config.SourceConfig;
import sources.deprecated.Attribute;

public interface Source {

    /**
     * Given a SourceConfig that configures access to a source, create necessary
     * tasks and submit them to the Conductor for processing
     * 
     * @param config
     * @param c
     */
    public List<Source> processSource(SourceConfig config);

    /**
     * Return the source type
     * 
     * @return
     */
    public SourceType getSourceType();

    /**
     * Obtain a path to a specific source resource
     * 
     * @return
     */
    public String getPath();

    /**
     * Obtain the name of the relation
     * 
     * @return
     */
    public String getRelationName();

    /**
     * Obtain the attributes for of the given source
     * 
     * @return
     * @throws IOException
     * @throws SQLException
     */
    public List<Attribute> getAttributes() throws IOException, SQLException;

    /**
     * Returns the original source config used to configure the current source
     * 
     * @return
     */
    public SourceConfig getSourceConfig();

    /**
     * If this works as a task, returns the task ID FIXME: this does not belong
     * here, need to find new iface
     * 
     * @return
     */
    public int getTaskId();

    /**
     * Reads the actual source and returns the values along with each
     * attribute's info
     * 
     * @param num
     * @return
     * @throws IOException
     * @throws SQLException
     */
    public Map<Attribute, List<String>> readRows(int num) throws IOException, SQLException;

    /**
     * Cease existing. Free all resources consumed by this Source
     */
    public void close();

}

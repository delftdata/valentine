package sources.implementations;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;

import metrics.Metrics;
import sources.Source;
import sources.SourceType;
import sources.SourceUtils;
import sources.config.HiveSourceConfig;
import sources.config.SourceConfig;
import sources.deprecated.Attribute;
import sources.deprecated.Record;
import sources.deprecated.TableInfo;

public class HiveSource implements Source {

    final private Logger LOG = LoggerFactory.getLogger(HiveSource.class.getName());

    private Connection connection;

    private String relationName;
    private int tid;
    private HiveSourceConfig config;

    private TableInfo tableInfo;
    private boolean firstTime = true;
    private Statement theStatement;
    private ResultSet theRS;

    // Metrics
    private Counter error_records = Metrics.REG.counter((name(PostgresSource.class, "error", "records")));
    private Counter success_records = Metrics.REG.counter((name(PostgresSource.class, "success", "records")));

    public HiveSource() {

    }

    public HiveSource(String relationName) {
	this.relationName = relationName;
	this.tid = SourceUtils.computeTaskId(config.getDatabase_name(), relationName);
    }

    @Override
    public List<Source> processSource(SourceConfig config) {
	assert (config instanceof HiveSourceConfig);

	HiveSourceConfig hiveConfig = (HiveSourceConfig) config;
	this.config = hiveConfig;

	List<Source> tasks = new ArrayList<>();

	// TODO: at this point we'll be harnessing metadata from the source

	String ip = hiveConfig.getHive_server_ip();
	String port = new Integer(hiveConfig.getHive_server_port()).toString();
	String dbName = hiveConfig.getDatabase_name();

	LOG.info("Conn to Hive on: {}:{}", ip, port);

	// FIXME: remove this enum; simplify this
	Connection hiveConn = SourceUtils.getDBConnection(SourceType.hive, ip, port, dbName, null, null);

	List<String> tables = SourceUtils.getTablesFromDatabase(hiveConn, null);
	try {
	    hiveConn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	for (String relationName : tables) {
	    LOG.info("Detected relational table: {}", relationName);

	    HiveSource hs = new HiveSource(relationName);

	    tasks.add(hs);
	}
	return tasks;
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.hive;
    }

    @Override
    public String getPath() {
	return config.getPath();
    }

    @Override
    public String getRelationName() {
	return this.relationName;
    }

    @Override
    public List<Attribute> getAttributes() throws IOException, SQLException {
	if (tableInfo.getTableAttributes() != null)
	    return tableInfo.getTableAttributes();
	DatabaseMetaData metadata = connection.getMetaData();
	ResultSet resultSet = metadata.getColumns(null, null, config.getRelationName(), null);
	Vector<Attribute> attrs = new Vector<Attribute>();
	while (resultSet.next()) {
	    String name = resultSet.getString("COLUMN_NAME");
	    String type = resultSet.getString("TYPE_NAME");
	    int size = resultSet.getInt("COLUMN_SIZE");
	    Attribute attr = new Attribute(name, type, size);
	    attrs.addElement(attr);
	}
	resultSet.close();
	tableInfo.setTableAttributes(attrs);
	return attrs;
    }

    @Override
    public SourceConfig getSourceConfig() {
	return this.config;
    }

    @Override
    public int getTaskId() {
	return this.tid;
    }

    @Override
    public Map<Attribute, List<String>> readRows(int num) throws IOException, SQLException {
	if (firstTime) {
	    handleFirstTime(num);
	    firstTime = false;
	}
	Map<Attribute, List<String>> data = new LinkedHashMap<>();
	// Make sure attrs is populated, if not, populate it here
	if (data.isEmpty()) {
	    List<Attribute> attrs = this.getAttributes();
	    attrs.forEach(a -> data.put(a, new ArrayList<>()));
	}

	// Read data and insert in order
	List<Record> recs = new ArrayList<>();
	boolean readData = this.read(num, recs);
	if (!readData) {
	    return null;
	}

	for (Record r : recs) {
	    List<String> values = r.getTuples();
	    int currentIdx = 0;
	    if (values.size() != data.values().size()) {
		error_records.inc();
		continue; // Some error while parsing data, a row has a
			  // different format
	    }
	    success_records.inc();
	    for (List<String> vals : data.values()) { // ordered iteration
		vals.add(values.get(currentIdx));
		currentIdx++;
	    }
	}
	return data;
    }

    private boolean read(int num, List<Record> rec_list) throws SQLException {
	if (firstTime) {
	    handleFirstTime(num);
	    firstTime = false;
	}

	boolean new_row = false;

	while (num > 0 && theRS.next()) { // while there are some available and
					  // we need to read more records
	    new_row = true;

	    num--;
	    // FIXME: profile and optimize this
	    Record rec = new Record();
	    for (int i = 0; i < this.tableInfo.getTableAttributes().size(); i++) {
		Object obj = theRS.getObject(i + 1);
		if (obj != null) {
		    String v1 = obj.toString();
		    rec.getTuples().add(v1);
		} else {
		    rec.getTuples().add("");
		}
	    }
	    rec_list.add(rec);
	}

	return new_row;
    }

    private boolean handleFirstTime(int fetchSize) {
	String sql = "SELECT * FROM " + config.getRelationName();

	try {
	    connection.setAutoCommit(false);
	    theStatement = connection.createStatement();
	    theStatement.setFetchSize(fetchSize);
	    theRS = theStatement.executeQuery(sql);
	} catch (SQLException sqle) {
	    System.out.println("ERROR: " + sqle.getLocalizedMessage());
	    return false;
	} catch (Exception e) {
	    System.out.println("ERROR: executeQuery failed");
	    return false;
	}
	return true;
    }

    @Override
    public void close() {
	try {
	    // this.connection.close();
	    this.theRS.close();
	    this.theStatement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

}

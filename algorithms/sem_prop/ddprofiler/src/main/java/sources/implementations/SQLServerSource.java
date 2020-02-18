package sources.implementations;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import metrics.Metrics;
import sources.Source;
import sources.SourceType;
import sources.SourceUtils;
import sources.config.SQLServerSourceConfig;
import sources.config.SourceConfig;
import sources.deprecated.Attribute;
import sources.deprecated.Record;
import sources.deprecated.TableInfo;

public class SQLServerSource implements Source {

    final private Logger LOG = LoggerFactory.getLogger(SQLServerSource.class.getName());

    private String relationName;
    private int tid;
    private SQLServerSourceConfig config;

    private TableInfo tableInfo;
    private Connection connection;
    private boolean initialized = false;
    private boolean firstTime = true;
    private Statement theStatement;
    private ResultSet theRS;

    // this was before in conductor, can we move it to 1 place per source?
    public static Map<String, Connection> connectionPools = new HashMap<>();

    // Metrics
    private Counter error_records = Metrics.REG.counter((name(PostgresSource.class, "error", "records")));
    private Counter success_records = Metrics.REG.counter((name(PostgresSource.class, "success", "records")));

    public SQLServerSource() {

    }

    public SQLServerSource(String relationName) {
	this.relationName = relationName;
	this.tid = SourceUtils.computeTaskId(config.getDatabase_name(), relationName);
    }

    @Override
    public List<Source> processSource(SourceConfig config) {
	assert (config instanceof SQLServerSourceConfig);

	SQLServerSourceConfig sqlServerConfig = (SQLServerSourceConfig) config;
	this.config = sqlServerConfig;

	List<Source> tasks = new ArrayList<>();

	// TODO: at this point we'll be harnessing metadata from the source

	String ip = sqlServerConfig.getDb_server_ip();
	String port = new Integer(sqlServerConfig.getDb_server_port()).toString();
	String db_name = sqlServerConfig.getDatabase_name();
	String username = sqlServerConfig.getDb_username();
	String password = sqlServerConfig.getDb_password();
	String dbschema = "default";

	LOG.info("Conn to DB on: {}:{}/{}", ip, port, db_name);

	Connection dbConn = SourceUtils.getDBConnection(SourceType.sqlserver, ip, port, db_name, username, password);

	List<String> tables = SourceUtils.getTablesFromDatabase(dbConn, dbschema);
	try {
	    dbConn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	for (String relationName : tables) {
	    LOG.info("Detected relational table: {}", relationName);
	    SQLServerSource sss = new SQLServerSource(relationName);
	    tasks.add(sss);
	}
	return tasks;
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.sqlserver;
    }

    @Override
    public String getPath() {
	return config.getPath();
    }

    @Override
    public String getRelationName() {
	return relationName;
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
	return config;
    }

    @Override
    public int getTaskId() {
	return tid;
    }

    private void initializeConnection() {
	// Definition of a conn identifier is here
	String ip = config.getDb_server_ip();
	String port = new Integer(config.getDb_server_port()).toString();
	String connPath = config.getDatabase_name();
	String username = config.getDb_username();
	String password = config.getDb_password();
	String dbName = config.getDatabase_name();

	String connIdentifier = config.getDatabase_name() + ip + port;

	if (this.connectionPools.containsKey(connIdentifier)) {
	    this.connection = this.connectionPools.get(connIdentifier);
	    return;
	}

	try {
	    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	} catch (ClassNotFoundException e1) {
	    // TODO Auto-generated catch block
	    e1.printStackTrace();
	}
	String cPath = "jdbc:sqlserver://" + ip + ":" + port + "; databaseName=" + dbName;

	// If no existing pool to handle this db, then we create a new one
	HikariConfig config = new HikariConfig();
	config.setJdbcUrl(cPath);
	config.setUsername(username);
	config.setPassword(password);
	config.addDataSourceProperty("cachePrepStmts", "true");
	config.addDataSourceProperty("prepStmtCacheSize", "250");
	config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
	config.addDataSourceProperty("maximumPoolSize", "1");
	HikariDataSource ds = new HikariDataSource(config);

	Connection connection = null;
	try {
	    connection = ds.getConnection();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	this.connectionPools.put(connIdentifier, connection);

	this.connection = connection;
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

    @Override
    public Map<Attribute, List<String>> readRows(int num) throws IOException, SQLException {
	if (!initialized) {
	    initializeConnection();
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

    private boolean handleFirstTime(int fetchSize) {
	String sql = "SELECT * FROM " + this.relationName;

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

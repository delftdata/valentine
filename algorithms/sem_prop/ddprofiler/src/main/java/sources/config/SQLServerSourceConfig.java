package sources.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import sources.SourceType;

public class SQLServerSourceConfig implements SourceConfig {

    private String sourceName;

    private String relationName;

    @JsonProperty
    private String db_server_ip;

    @JsonProperty
    private int db_server_port;

    @JsonProperty
    private String database_name;

    @JsonProperty
    private String db_username;

    @JsonProperty
    private String db_password;

    private String path;

    public String getDb_server_ip() {
	return db_server_ip;
    }

    public int getDb_server_port() {
	return db_server_port;
    }

    public String getDatabase_name() {
	return database_name;
    }

    public String getDb_username() {
	return db_username;
    }

    public String getDb_password() {
	return db_password;
    }

    @Override
    public String getSourceName() {
	return sourceName;
    }

    @Override
    public String getPath() {
	path = "jdbc:sqlserver://" + db_server_ip + ":" + db_server_port + "; " + "databaseName=" + database_name
		+ "; user=" + db_username + "; password=" + db_password + ";";
	return path;
    }

    @Override
    public void setSourceName(String sourceName) {
	this.sourceName = sourceName;
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.sqlserver;
    }

    public String getRelationName() {
	return relationName;
    }

    public void setRelationName(String relationName) {
	this.relationName = relationName;
    }

    @Override
    public SourceConfig selfCopy() {
	SQLServerSourceConfig copy = new SQLServerSourceConfig();

	copy.sourceName = this.sourceName;
	copy.relationName = this.relationName;
	copy.database_name = this.database_name;
	copy.db_server_ip = this.db_server_ip;
	copy.db_server_port = this.db_server_port;
	copy.db_username = this.db_username;
	copy.db_password = this.db_password;

	return copy;
    }

}

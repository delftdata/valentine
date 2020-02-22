package sources.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import sources.SourceType;

public class HiveSourceConfig implements SourceConfig {

    private String sourceName;

    private String relationName;

    @JsonProperty
    private String hive_server_ip;

    @JsonProperty
    private int hive_server_port;

    @JsonProperty
    private String path;

    @JsonProperty
    private String database_name;

    // @JsonProperty
    // private String db_username;
    //
    // @JsonProperty
    // private String db_password;

    public String getHive_server_ip() {
	return hive_server_ip;
    }

    public int getHive_server_port() {
	return hive_server_port;
    }

    public String getDatabase_name() {
	return database_name;
    }

    @Override
    public String getPath() {
	return path;
    }

    //
    // public String getDb_username() {
    // return db_username;
    // }
    //
    // public String getDb_password() {
    // return db_password;
    // }

    @Override
    public String getSourceName() {
	return sourceName;
    }

    @Override
    public void setSourceName(String sourceName) {
	this.sourceName = sourceName;
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.hive;
    }

    public String getRelationName() {
	return relationName;
    }

    public void setRelationName(String relationName) {
	this.relationName = relationName;
    }

    @Override
    public SourceConfig selfCopy() {
	HiveSourceConfig copy = new HiveSourceConfig();

	copy.sourceName = this.sourceName;
	copy.relationName = this.relationName;
	// copy.database_name = this.database_name;
	copy.hive_server_ip = this.hive_server_ip;
	copy.hive_server_port = this.hive_server_port;
	// copy.db_username = this.db_username;
	// copy.db_password = this.db_password;

	return copy;
    }

}

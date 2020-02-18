package sources;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import sources.config.CSVSourceConfig;
import sources.config.GenericSource;
import sources.config.PostgresSourceConfig;
import sources.config.SourceConfig;
import sources.config.Sources;

public class YAMLParser {

    final private static Logger LOG = LoggerFactory.getLogger(YAMLParser.class.getName());

    public static List<SourceConfig> processSourceConfig(String path) {
	File file = new File(path);
	Sources srcs = null;
	ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
	try {
	    srcs = mapper.readValue(file, Sources.class);
	} catch (Exception e) {
	    LOG.error("While parsing {} file to read sources", file.toPath());
	    e.printStackTrace();
	    System.exit(0);
	}
	int apiVersion = srcs.getApi_version();
	assert (apiVersion == 0); // to support api evolution

	List<SourceConfig> sourceConfigs = new ArrayList<>();

	// Parse every source in the file
	List<GenericSource> sources = srcs.getSources();
	for (GenericSource src : sources) {
	    String name = src.getName();
	    SourceType type = src.getType();
	    JsonNode props = src.getConfig();
	    SourceConfig sc = null;

	    sc = SourceType.map(type, props, mapper);

	    sc.setSourceName(name);
	    sourceConfigs.add(sc);
	}

	return sourceConfigs;
    }

    public static void main(String args[]) {

	// FIXME: make this a test instead
	File file = new File("/Users/ra-mit/development/discovery_proto/ddprofiler/src/main/resources/template.yml");
	Sources srcs = null;
	ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
	try {
	    srcs = mapper.readValue(file, Sources.class);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	System.out.println("api_version: " + srcs.getApi_version());
	List<GenericSource> sources = srcs.getSources();
	for (GenericSource src : sources) {
	    String name = src.getName();
	    SourceType type = src.getType();
	    JsonNode props = src.getConfig();
	    System.out.println("name: " + name);
	    System.out.println("type: " + type);
	    if (type == SourceType.csv) {
		CSVSourceConfig csvSource = mapper.convertValue(props, CSVSourceConfig.class);
		String path = csvSource.getPath();
		String separator = csvSource.getSeparator();
		System.out.println(path);
		System.out.println(separator);
	    }
	    if (type == SourceType.postgres) {
		PostgresSourceConfig postgresSource = mapper.convertValue(props, PostgresSourceConfig.class);
		String databaseName = postgresSource.getDatabase_name();
		String db_server_ip = postgresSource.getDb_server_ip();
		int db_server_port = postgresSource.getDb_server_port();
		String db_username = postgresSource.getDb_username();
		String db_password = postgresSource.getDb_password();
		System.out.println(databaseName);
		System.out.println(db_server_ip);
		System.out.println(db_server_port);
		System.out.println(db_username);
		System.out.println(db_password);
	    }
	}

    }
}

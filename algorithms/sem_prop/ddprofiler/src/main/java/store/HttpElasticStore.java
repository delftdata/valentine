package store;

import java.util.List;

import core.WorkerTaskResult;
import core.config.ProfilerConfig;

public class HttpElasticStore implements Store {

    // TODO: placeholder class for future http high level rest api (elastic 5.6.0+)

    private String serverUrl;

    public HttpElasticStore(ProfilerConfig pc) {
	String storeServer = pc.getString(ProfilerConfig.STORE_SERVER);
	int storePort = pc.getInt(ProfilerConfig.STORE_PORT);
	this.serverUrl = "http://" + storeServer + ":" + storePort;
    }

    @Override
    public void initStore() {
	// TODO:
    }

    @Override
    public boolean indexData(long id, String dbName, String path, String sourceName, String columnName,
	    List<String> values) {
	// TODO:
	return true;
    }

    @Override
    public boolean storeDocument(WorkerTaskResult wtr) {
	// TODO:
	return true;
    }

    @Override
    public void tearDownStore() {
	// TODO
    }

    private void silenceJestLogger() {
	final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("io.searchbox.client");
	final org.slf4j.Logger logger2 = org.slf4j.LoggerFactory.getLogger("io.searchbox.action");
	if (!(logger instanceof ch.qos.logback.classic.Logger)) {
	    return;
	}
	if (!(logger2 instanceof ch.qos.logback.classic.Logger)) {
	    return;
	}
	ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
	ch.qos.logback.classic.Logger logbackLogger2 = (ch.qos.logback.classic.Logger) logger2;
	logbackLogger.setLevel(ch.qos.logback.classic.Level.INFO);
	logbackLogger2.setLevel(ch.qos.logback.classic.Level.INFO);
    }

}

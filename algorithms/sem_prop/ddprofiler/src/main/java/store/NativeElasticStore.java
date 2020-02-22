package store;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.WorkerTaskResult;
import core.config.ProfilerConfig;

public class NativeElasticStore implements Store {

    final private Logger LOG = LoggerFactory.getLogger(NativeElasticStore.class.getName());

    private String serverUrl;
    private String storeServer;
    private int storePort;

    private TransportClient client;
    // private Client nativeClient;
    private BulkProcessor bulkProcessor;

    public NativeElasticStore(ProfilerConfig pc) {
	String storeServer = pc.getString(ProfilerConfig.STORE_SERVER);
	int storePort = pc.getInt(ProfilerConfig.STORE_PORT);
	int storeHttpPort = pc.getInt(ProfilerConfig.STORE_HTTP_PORT);
	this.storeServer = storeServer;
	this.storePort = storePort;
	this.serverUrl = "http://" + storeServer + ":" + storeHttpPort;
    }

    @Override
    public void initStore() {
	// Create native client
	try {
	    Settings settings = Settings.builder().put("client.transport.sniff", false)
		    .put("client.transport.ignore_cluster_name", true).build();
	    client = new PreBuiltTransportClient(settings)
		    .addTransportAddress(new TransportAddress(InetAddress.getByName(storeServer), storePort));
	} catch (UnknownHostException e) {
	    e.printStackTrace();
	}

	// Create bulk processor
	bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {

	    private long startRequest;
	    private long endRequest;

	    @Override
	    public void beforeBulk(long executionId, BulkRequest request) {
		startRequest = System.currentTimeMillis();
	    }

	    @Override
	    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
		endRequest = System.currentTimeMillis();
		LOG.info("Done bulk index request, took: {}", (endRequest - startRequest));
	    }

	    @Override
	    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
		LOG.error("FAILED? " + request.toString());
		LOG.error(failure.getMessage());
	    }

	}).setBulkActions(-1).setBulkSize(new ByteSizeValue(50, ByteSizeUnit.MB))
		.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1) // Means
											  // requests
											  // are
											  // queud
											  // while
											  // a
											  // bulkrequest
											  // is
											  // in
											  // progress
		.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) // just
												       // default
		.build();

	XContentBuilder set = null;
	try {
	    set = jsonBuilder().startObject().startObject("analysis");

	    set.startObject("char_filter");
	    // set.startObject("_to-");
	    set.startObject("aurum_char_filter");
	    set.field("type", "mapping").field("mappings").startArray().value("_=>-").value(".csv=> ").endArray();
	    set.endObject();
	    set.endObject();

	    set.startObject("filter");
	    set.startObject("english_stop");
	    set.field("type", "stop").field("stopwords", "_english_");
	    set.endObject();
	    set.startObject("english_stemmer");
	    set.field("type", "stemmer").field("language", "english");
	    set.endObject();
	    set.startObject("english_possessive_stemmer");
	    set.field("type", "stemmer").field("language", "possessive_english");
	    set.endObject();
	    set.endObject(); // closes filter

	    set.startObject("analyzer");
	    set.startObject("aurum_analyzer");
	    set.field("tokenizer", "standard");
	    set.field("char_filter").startArray().value("aurum_char_filter").endArray();
	    // .value("_to-").value("csv_to_none").endArray();
	    set.field("filter").startArray().value("english_possessive_stemmer").value("lowercase")
		    .value("english_stop").value("english_stemmer").endArray();
	    set.endObject(); // closes aurum_analyzer
	    set.endObject(); // closes analyzer

	    set.endObject(); // closes analysis
	    set.endObject(); // closes object
	} catch (IOException e) {
	    e.printStackTrace();
	}

	XContentBuilder text_mapping = null;
	try {
	    text_mapping = jsonBuilder().startObject().startObject("properties");
	    text_mapping.startObject("id");
	    text_mapping.field("type", "long").field("store", "true").field("index", "true");
	    text_mapping.endObject();
	    text_mapping.startObject("dbName");
	    text_mapping.field("type", "keyword").field("index", "false");
	    text_mapping.endObject();
	    text_mapping.startObject("path");
	    text_mapping.field("type", "keyword").field("index", "false");
	    text_mapping.endObject();
	    text_mapping.startObject("sourceName");
	    text_mapping.field("type", "keyword").field("index", "false");
	    text_mapping.endObject();
	    text_mapping.startObject("columnName");
	    text_mapping.field("type", "keyword").field("index", "false");
	    text_mapping.field("ignore_above", "512");
	    text_mapping.endObject();
	    text_mapping.startObject("columnNameSuggest");
	    text_mapping.field("type", "completion");
	    // .field("analyzer", "aurum_analyzer");
	    text_mapping.endObject();
	    text_mapping.startObject("text");
	    text_mapping.field("type", "text").field("store", "false");
	    text_mapping.field("index", "true").field("analyzer", "english");
	    text_mapping.field("term_vector", "yes");
	    text_mapping.endObject();

	    text_mapping.endObject(); // close properties
	    text_mapping.endObject(); // close mapping
	} catch (IOException e) {
	    e.printStackTrace();
	}

	XContentBuilder profile_mapping = null;
	try {
	    profile_mapping = jsonBuilder().startObject().startObject("properties");
	    profile_mapping.startObject("id");
	    profile_mapping.field("type", "long").field("index", "true");
	    profile_mapping.endObject();
	    profile_mapping.startObject("dbName");
	    profile_mapping.field("type", "keyword").field("index", "false");
	    profile_mapping.endObject();
	    profile_mapping.startObject("path");
	    profile_mapping.field("type", "keyword").field("index", "false");
	    profile_mapping.endObject();
	    profile_mapping.startObject("sourceNameNA");
	    profile_mapping.field("type", "keyword").field("index", "true");
	    profile_mapping.endObject();
	    profile_mapping.startObject("sourceName");
	    profile_mapping.field("type", "text").field("index", "true").field("analyzer", "aurum_analyzer");
	    profile_mapping.endObject();
	    profile_mapping.startObject("columnNameNA");
	    profile_mapping.field("type", "keyword").field("index", "true");
	    profile_mapping.endObject();
	    profile_mapping.startObject("columnName");
	    profile_mapping.field("type", "text").field("index", "true").field("analyzer", "aurum_analyzer");
	    profile_mapping.endObject();
	    profile_mapping.startObject("dataType");
	    profile_mapping.field("type", "keyword").field("index", "true");
	    profile_mapping.endObject();
	    profile_mapping.startObject("totalValues");
	    profile_mapping.field("type", "long").field("index", "false");
	    profile_mapping.endObject();
	    profile_mapping.startObject("uniqueValues");
	    profile_mapping.field("type", "long").field("index", "false");
	    profile_mapping.endObject();
	    profile_mapping.startObject("entities");
	    profile_mapping.field("type", "keyword").field("index", "true");
	    profile_mapping.endObject();
	    profile_mapping.startObject("minhash");
	    profile_mapping.field("type", "long").field("index", "false");
	    profile_mapping.endObject();
	    profile_mapping.startObject("minValue");
	    profile_mapping.field("type", "double").field("index", "false");
	    profile_mapping.endObject();
	    profile_mapping.startObject("maxValue");
	    profile_mapping.field("type", "double").field("index", "false");
	    profile_mapping.endObject();
	    profile_mapping.startObject("avgValue");
	    profile_mapping.field("type", "double").field("index", "false");
	    profile_mapping.endObject();
	    profile_mapping.startObject("median");
	    profile_mapping.field("type", "long").field("index", "false");
	    profile_mapping.endObject();
	    profile_mapping.startObject("iqr");
	    profile_mapping.field("type", "long").field("index", "false");
	    profile_mapping.endObject();

	    profile_mapping.endObject(); // close properties
	    profile_mapping.endObject(); // close mapping
	} catch (IOException e) {
	    e.printStackTrace();
	}

	// Obtain indices client
	IndicesAdminClient admin = client.admin().indices();

	// Check if indices exist already, somehow this op is not idempotent
	IndicesExistsResponse doesExist = admin.prepareExists("text", "profile").get();
	if (!doesExist.isExists()) {
	    LOG.info("Indices do not exist, creating indices and mappings");
	    // Create indexes and apply settings and mappings
	    admin.prepareCreate("text").addMapping("column", text_mapping).get();
	    admin.preparePutMapping("text").setType("column").setSource(text_mapping).get();
	    admin.prepareCreate("profile").addMapping("column", profile_mapping).setSettings(set).get();
	    admin.preparePutMapping("profile").setType("column").setSource(profile_mapping).get();
	} else {
	    LOG.info("Indices already exist, moving on");
	}
    }

    @Override
    public boolean indexData(long id, String dbName, String path, String sourceName, String columnName,
	    List<String> values) {

	XContentBuilder builder = null;
	try {
	    builder = jsonBuilder().startObject().field("id", id).field("dbName", dbName).field("path", path)
		    .field("sourceName", sourceName).field("columnName", columnName)
		    .field("columnNameSuggest", columnName).startArray("text");

	    for (String v : values) {
		builder.value(v);
	    }

	    builder.endArray().endObject();
	} catch (IOException e) {
	    e.printStackTrace();
	}

	// Using bulkProcessor
	IndexRequest ir = new IndexRequest("text", "column").source(builder);
	bulkProcessor.add(ir);

	return true;
    }

    @Override
    public boolean storeDocument(WorkerTaskResult wtr) {
	String strId = Long.toString(wtr.getId());

	XContentBuilder builder = null;
	try {
	    builder = jsonBuilder().startObject().field("id", wtr.getId()).field("dbName", wtr.getDBName())
		    .field("path", wtr.getPath()).field("sourceName", wtr.getSourceName())
		    .field("columnNameNA", wtr.getColumnName()).field("columnName", wtr.getColumnName())
		    .field("dataType", wtr.getDataType()).field("totalValues", wtr.getTotalValues())
		    .field("uniqueValues", wtr.getUniqueValues()).field("entities", wtr.getEntities().toString())

		    .startArray("minhash");

	    long[] mh = wtr.getMH();
	    if (mh != null) { // that's an integer column
		for (long i : mh) {
		    builder.value(i);
		}
	    } else {
		builder.value(-1);
	    }

	    builder.endArray()

		    .field("minValue", wtr.getMinValue()).field("maxValue", wtr.getMaxValue())
		    .field("avgValue", wtr.getAvgValue()).field("median", wtr.getMedian()).field("iqr", wtr.getIQR())
		    .endObject();
	} catch (IOException e) {
	    e.printStackTrace();
	}

	IndexRequest ir = new IndexRequest("profile", "column", strId).source(builder);

	bulkProcessor.add(ir);

	return true;
    }

    @Override
    public void tearDownStore() {
	bulkProcessor.close();
    }

}

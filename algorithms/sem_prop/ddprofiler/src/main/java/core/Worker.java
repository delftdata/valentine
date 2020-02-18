package core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analysis.Analysis;
import analysis.AnalyzerFactory;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import analysis.modules.EntityAnalyzer;
import core.config.ProfilerConfig;
import preanalysis.PreAnalyzer;
import preanalysis.Values;
import sources.Source;
import sources.deprecated.Attribute;
import sources.deprecated.Attribute.AttributeType;
import sources.deprecated.BenchmarkingData;
import store.Store;

public class Worker implements Runnable {

    final private Logger LOG = LoggerFactory.getLogger(Worker.class.getName());
    private ProfilerConfig pc;

    private final int pseudoRandomSeed = 1;

    private Conductor conductor;
    private boolean doWork = true;
    private String workerName;
    private Source task;
    private int numRecordChunk;
    private Store store;

    private BlockingQueue<Source> taskQueue;
    private BlockingQueue<ErrorPackage> errorQueue;

    // Benchmark variables
    private boolean first = true;
    private BenchmarkingData benchData;

    // cached object
    private EntityAnalyzer ea;

    public Worker(Conductor conductor, ProfilerConfig pc, String workerName, BlockingQueue<Source> taskQueue,
	    BlockingQueue<ErrorPackage> errorQueue, Store store, EntityAnalyzer cached) {
	this.conductor = conductor;
	this.numRecordChunk = pc.getInt(ProfilerConfig.NUM_RECORD_READ);
	this.store = store;
	this.ea = cached;
	this.workerName = workerName;
	this.taskQueue = taskQueue;
	this.errorQueue = errorQueue;
	this.pc = pc;
    }

    public void stop() {
	this.doWork = false;
    }

    private Source pullTask() {
	// Attempt to consume new task
	Source task = null;
	try {
	    task = taskQueue.poll(500, TimeUnit.MILLISECONDS);
	    if (task == null) {
		return null;
	    }
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
	return task;
    }

    @Override
    public void run() {

	while (doWork) {
	    try {
		// Collection to hold analyzers
		Map<String, Analysis> analyzers = new HashMap<>();

		task = pullTask();

		if (task == null) {
		    continue;
		}

		String path = task.getPath();
		DataIndexer indexer = new FilterAndBatchDataIndexer(store, task.getSourceConfig().getSourceName(), path,
			task.getRelationName());

		// Access attributes and attribute type through first read
		PreAnalyzer pa = new PreAnalyzer(pc);
		pa.assignSourceTask(task);

		LOG.info("Worker: {} processing: {}", workerName, task.getRelationName());

		Map<Attribute, Values> initData = pa.readRows(numRecordChunk);
		if (initData == null) {
		    LOG.warn("No data read from: {}", task.getSourceConfig().getRelationName());
		    task.close();
		}

		// Read initial records to figure out attribute types etc
		// FIXME: readFirstRecords(initData, analyzers);
		readFirstRecords(task.getSourceConfig().getSourceName(), path, initData, analyzers, indexer);

		// Consume all remaining records from the connector
		Map<Attribute, Values> data = pa.readRows(numRecordChunk);
		int records = 0;
		while (data != null) {
		    indexer.indexData(task.getSourceConfig().getSourceName(), path, data);
		    records = records + data.size();
		    Conductor.recordsPerSecond.mark(records);
		    // Do the processing
		    // FIXME: feedValuesToAnalyzers(data, analyzers);
		    feedValuesToAnalyzers(data, analyzers);

		    // Read next chunk of data
		    data = pa.readRows(numRecordChunk);
		}

		// Get results and wrap them in a Result object
		// FIXME: WorkerTaskResultHolder wtrf = new
		// WorkerTaskResultHolder(c.getSourceName(), c.getAttributes(),
		// analyzers);
		WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(task.getSourceConfig().getSourceName(), path,
			task.getRelationName(), task.getAttributes(), analyzers);

		// List<WorkerTaskResult> rs =
		// WorkerTaskResultHolder.makeFakeOne();
		// WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(rs);

		// // FIXME: indexer.flushAndClose();
		task.close();
		List<WorkerTaskResult> results = wtrf.get();

		for (WorkerTaskResult wtr : results) {
		    store.storeDocument(wtr);
		}

		conductor.notifyProcessedTask(results.size());
	    } catch (Exception e) {
		String init = "#########";
		String msg = task.toString() + " $FAILED$ cause-> " + e.getMessage();
		StackTraceElement[] trace = e.getStackTrace();
		StringBuffer sb = new StringBuffer();
		sb.append(init);
		sb.append(System.lineSeparator());
		sb.append(msg);
		sb.append(System.lineSeparator());
		for (int i = 0; i < trace.length; i++) {
		    sb.append(trace[i].toString());
		    sb.append(System.lineSeparator());
		}
		sb.append(System.lineSeparator());
		String log = sb.toString();
		try {
		    errorQueue.put(new ErrorPackage(log));
		} catch (InterruptedException e1) {
		    e1.printStackTrace();
		}
	    }
	}
	LOG.info("THREAD: {} stopping", workerName);
    }

    private void readFirstRecords(String dbName, String path, Map<Attribute, Values> initData,
	    Map<String, Analysis> analyzers, DataIndexer indexer) {
	for (Entry<Attribute, Values> entry : initData.entrySet()) {
	    Attribute a = entry.getKey();
	    AttributeType at = a.getColumnType();
	    Analysis analysis = null;
	    if (at.equals(AttributeType.FLOAT)) {
		analysis = AnalyzerFactory.makeNumericalAnalyzer();
		((NumericalAnalysis) analysis).feedFloatData(entry.getValue().getFloats());
	    } else if (at.equals(AttributeType.INT)) {
		analysis = AnalyzerFactory.makeNumericalAnalyzer();
		((NumericalAnalysis) analysis).feedIntegerData(entry.getValue().getIntegers());
	    } else if (at.equals(AttributeType.STRING)) {
		analysis = AnalyzerFactory.makeTextualAnalyzer(ea, pseudoRandomSeed);
		((TextualAnalysis) analysis).feedTextData(entry.getValue().getStrings());
	    }
	    analyzers.put(a.getColumnName(), analysis);
	}

	// Index text read so far
	indexer.indexData(dbName, path, initData);
    }

    private void feedValuesToAnalyzers(Map<Attribute, Values> data, Map<String, Analysis> analyzers) {
	// Do the processing
	for (Entry<Attribute, Values> entry : data.entrySet()) {
	    String atName = entry.getKey().getColumnName();
	    Values vs = entry.getValue();
	    if (vs.areFloatValues()) {
		((NumericalAnalysis) analyzers.get(atName)).feedFloatData(vs.getFloats());
	    } else if (vs.areIntegerValues()) {
		((NumericalAnalysis) analyzers.get(atName)).feedIntegerData(vs.getIntegers());
	    } else if (vs.areStringValues()) {
		((TextualAnalysis) analyzers.get(atName)).feedTextData(vs.getStrings());
	    }
	}
    }

}

package core;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import analysis.modules.EntityAnalyzer;
import core.config.ProfilerConfig;
import metrics.Metrics;
import opennlp.tools.namefind.TokenNameFinderModel;
import sources.Source;
import store.Store;

public class Conductor {

    final private Logger LOG = LoggerFactory.getLogger(Conductor.class.getName());

    private ProfilerConfig pc;
    private File errorLogFile;

    private BlockingQueue<Source> taskQueue;
    private List<Worker> activeWorkers;
    private List<Thread> workerPool;
    private BlockingQueue<WorkerTaskResult> results;
    private BlockingQueue<ErrorPackage> errorQueue;

    private Store store;

    private Thread consumer;
    private Consumer runnable;
    // Cached entity analyzers (expensive initialization)
    private Map<String, EntityAnalyzer> cachedEntityAnalyzers;

    // Metrics
    private int totalTasksSubmitted = 0;
    private int totalFailedTasks = 0;
    private AtomicInteger totalProcessedTasks = new AtomicInteger();
    private AtomicInteger totalColumns = new AtomicInteger();
    private Meter m;
    public static Meter recordsPerSecond;

    // Global cache, FIXME; find better place
    // TODO: move to some db-specific class
    @Deprecated
    public static Map<String, Connection> connectionPools = new HashMap<>();

    public Conductor(ProfilerConfig pc, Store s) {
	this.pc = pc;
	this.store = s;
	this.taskQueue = new LinkedBlockingQueue<>();
	this.results = new LinkedBlockingQueue<>();
	this.errorQueue = new LinkedBlockingQueue<>();

	int numWorkers = pc.getInt(ProfilerConfig.NUM_POOL_THREADS);
	this.workerPool = new ArrayList<>();
	this.activeWorkers = new ArrayList<>();
	List<TokenNameFinderModel> modelList = new ArrayList<>();
	List<String> modelNameList = new ArrayList<>();
	EntityAnalyzer first = new EntityAnalyzer();
	modelList = first.getCachedModelList();
	modelNameList = first.getCachedModelNameList();
	for (int i = 0; i < numWorkers; i++) {
	    String name = "Worker-" + new Integer(i).toString();
	    EntityAnalyzer cached = new EntityAnalyzer(modelList, modelNameList);
	    Worker w = new Worker(this, pc, name, taskQueue, errorQueue, store, cached);
	    Thread t = new Thread(w, name);
	    workerPool.add(t);
	    activeWorkers.add(w);
	}

	this.runnable = new Consumer();
	this.consumer = new Thread(runnable);
	String errorLogFileName = pc.getString(ProfilerConfig.ERROR_LOG_FILE_NAME);
	this.errorLogFile = new File(errorLogFileName);

	// Metrics
	m = Metrics.REG.meter(name(Conductor.class, "tasks", "per", "sec"));
	recordsPerSecond = Metrics.REG.meter(name(Conductor.class, "records", "per", "sec"));
    }

    public void start() {
	this.store.initStore();
	this.consumer.start();
    }

    public void stop() {
	this.runnable.stop();
	try {
	    this.consumer.join();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
    }

    public boolean submitTask(Source task) {
	totalTasksSubmitted++;
	return taskQueue.add(task);
    }

    public int approxQueueLenght() {
	return taskQueue.size();
    }

    public boolean isTherePendingWork() {
	return this.totalProcessedTasks.get() < this.totalTasksSubmitted;
    }

    public List<WorkerTaskResult> consumeResults() {
	List<WorkerTaskResult> availableResults = new ArrayList<>();
	WorkerTaskResult wtr = null;
	do {
	    try {
		wtr = results.poll(500, TimeUnit.MILLISECONDS);
		if (wtr != null) {
		    availableResults.add(wtr);
		}
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	} while (wtr != null);
	return availableResults;
    }

    public void notifyProcessedTask(int numCols) {
	totalProcessedTasks.incrementAndGet();
	m.mark();
	LOG.info(" {}/{} ", totalProcessedTasks, totalTasksSubmitted);
	LOG.info(" Failed tasks: {} ", totalFailedTasks);
	totalColumns.addAndGet(numCols);
	LOG.info("Added: {} cols, total processed: {} ", numCols, totalColumns);
	LOG.info("");
    }

    class Consumer implements Runnable {

	private boolean doWork = true;

	public void stop() {
	    doWork = false;
	}

	@Override
	public void run() {

	    // Start workers
	    for (Thread worker : workerPool) {
		worker.start();
	    }

	    while (doWork) {

		ErrorPackage ep;
		try {
		    ep = errorQueue.poll(1000, TimeUnit.MILLISECONDS);
		    if (ep != null) {
			String msg = ep.getErrorLog();
			Utils.appendLineToFile(errorLogFile, msg);
			LOG.warn(msg);
			totalProcessedTasks.incrementAndGet(); // other
							       // processed/failed
							       // task
			totalFailedTasks++;
		    }
		} catch (InterruptedException e) {
		    e.printStackTrace();
		}
	    }

	    // Stop workers
	    for (Worker w : activeWorkers) {
		w.stop();
	    }

	    LOG.info("Consumer stopping");
	}
    }
}

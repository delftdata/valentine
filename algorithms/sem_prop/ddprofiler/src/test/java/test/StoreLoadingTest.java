package test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import core.Conductor;
import core.WorkerTaskResult;
import core.config.ProfilerConfig;
import store.Store;
import store.StoreFactory;

public class StoreLoadingTest {

    // private String path = "/Users/ra-mit/Desktop/mitdwh_test/";
    private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
    private String filename = "short_cis_course_catalog.csv";
    private String separator = ",";

    @Test
    public void storeLoadingE2ETest() {

	Properties p = new Properties();
	p.setProperty(ProfilerConfig.NUM_POOL_THREADS, "12");
	p.setProperty(ProfilerConfig.NUM_RECORD_READ, "500");
	ProfilerConfig pc = new ProfilerConfig(p);

	// Create store
	Store elasticStore = StoreFactory.makeHttpElasticStore(pc);
	// Store elasticStore = StoreFactory.makeNullStore(pc);

	Conductor c = new Conductor(pc, elasticStore);
	c.start();
	try {
	    Files.walk(Paths.get(path)).forEach(filePath -> {
		if (Files.isRegularFile(filePath)) {
		    String name = filePath.getFileName().toString();
		    if (!name.equals(".DS_Store")) { // Make sure only valid
						     // files are in
						     // the folder
			// TaskPackage tp =
			// TaskPackage.makeCSVFileTaskPackage("", path, name,
			// separator);
			// c.submitTask(tp);
		    }
		}
	    });
	} catch (IOException e) {
	    e.printStackTrace();
	}

	long start = System.currentTimeMillis();
	while (c.isTherePendingWork()) {
	    List<WorkerTaskResult> results = null;
	    do {
		results = c.consumeResults(); // we know there is only one set
					      // of results
	    } while (results.isEmpty());

	    for (WorkerTaskResult wtr : results) {
		elasticStore.storeDocument(wtr);
	    }
	}
	long end = System.currentTimeMillis();

	System.out.println("DONE: " + (end - start));
    }
}

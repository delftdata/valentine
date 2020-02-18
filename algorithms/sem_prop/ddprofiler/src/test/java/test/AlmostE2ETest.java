package test;

import java.util.List;
import java.util.Properties;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import core.Conductor;
import core.WorkerTaskResult;
import core.config.ProfilerConfig;
import store.Store;
import store.StoreFactory;

public class AlmostE2ETest {

    private String path = "C:\\";
    private String filename = "Leading_Causes_of_Death__1990-2010.csv";
    // private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
    // private String filename = "short_cis_course_catalog.csv";
    private String separator = ",";

    private String db = "mysql";
    private String connIP = "localhost";
    private String port = "3306";
    private String sourceName = "/test";
    private String tableName = "nellsimple";
    private String username = "root";
    private String password = "Qatar";

    private ObjectMapper om = new ObjectMapper();

    public void finishTasks(Conductor c) {
	List<WorkerTaskResult> results = null;
	do {
	    results = c.consumeResults(); // we know there is only one set of
					  // results
	} while (results.isEmpty());

	for (WorkerTaskResult wtr : results) {
	    String textual = null;
	    try {
		textual = om.writeValueAsString(wtr);
	    } catch (JsonProcessingException e) {
		e.printStackTrace();
	    }
	    System.out.println(textual);
	}
    }

    @Test
    public void almostE2ETestDB() {

	Properties p = new Properties();
	p.setProperty(ProfilerConfig.NUM_POOL_THREADS, "1");
	p.setProperty(ProfilerConfig.NUM_RECORD_READ, "500");
	ProfilerConfig pc = new ProfilerConfig(p);
	Store es = StoreFactory.makeNullStore(pc);
	Conductor c = new Conductor(pc, es);

	c.start();

	// TaskPackage tp =
	// TaskPackage.makeCSVFileTaskPackage("", path, filename, separator);
	// c.submitTask(tp);
	finishTasks(c);
    }
}

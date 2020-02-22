package test.performance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import core.config.ProfilerConfig;
import preanalysis.PreAnalyzer;
import preanalysis.Values;
import sources.deprecated.Attribute;
import sources.implementations.CSVSource;

public class JustReadTest {

    private String path = "/Users/ra-mit/Desktop/mitdwh_test/";
    private String filename = "short_cis_course_catalog.csv";
    private String separator = ",";

    @Test
    public void test() throws IOException {
	Properties p = new Properties();
	p.setProperty(ProfilerConfig.NUM_POOL_THREADS, "8");
	int numRecordChunk = 500;
	p.setProperty(ProfilerConfig.NUM_RECORD_READ, "500");
	ProfilerConfig pc = new ProfilerConfig(p);

	int iterations = 24;
	long start = System.currentTimeMillis();
	while (iterations > 0) {
	    Files.walk(Paths.get(path)).forEach(filePath -> {
		if (Files.isRegularFile(filePath)) {
		    String name = filePath.getFileName().toString();
		    // FIXME: create config on the fly
		    CSVSource fc = new CSVSource();
		    try {
			fc = new CSVSource();
		    } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		    CSVSource c = fc;
		    PreAnalyzer pa = new PreAnalyzer(null);
		    pa.assignSourceTask(c);

		    // Consume all remaining records from the connector
		    Map<Attribute, Values> data = pa.readRows(numRecordChunk);
		    int records = 0;
		    while (data != null) {
			records = records + data.size();
			// Read next chunk of data
			data = pa.readRows(numRecordChunk);
		    }
		}
	    });
	    iterations--;
	}
	long end = System.currentTimeMillis();
	System.out.println("Total time: " + (end - start));
    }
}

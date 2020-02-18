package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import analysis.modules.RangeAnalyzer;
import preanalysis.PreAnalyzer;
import preanalysis.Values;
import sources.deprecated.Attribute;
import sources.deprecated.Attribute.AttributeType;
import sources.implementations.CSVSource;

public class RangeAnalyzerTest {

    private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
    private String filename = "short_cis_course_catalog.csv";
    private String separator = ",";
    private int numRecords = 100;

    @Test
    public void RangeTest() throws IOException {

	// FIXME: create config on the fly
	CSVSource fc = new CSVSource();
	PreAnalyzer pa = new PreAnalyzer(null);
	pa.assignSourceTask(fc);

	Map<Attribute, Values> data = pa.readRows(numRecords);

	for (Entry<Attribute, Values> a : data.entrySet()) {
	    AttributeType at = a.getKey().getColumnType();
	    RangeAnalyzer ra = new RangeAnalyzer();
	    if (at.equals(AttributeType.FLOAT)) {
		List<Float> floats = new ArrayList<>();
		for (Float s : a.getValue().getFloats()) {
		    floats.add(s);
		}

		ra.feedFloatData(floats);

		long q25 = ra.getQuantile(0.25);
		long q50 = ra.getQuantile(0.5);
		long q75 = ra.getQuantile(0.75);
		System.out.println(a.toString());
		System.out.println(q25 + " - " + q50 + " - " + q75);
	    } else if (at.equals(AttributeType.INT)) {
		List<Long> integers = new ArrayList<>();
		for (Long s : a.getValue().getIntegers()) {
		    integers.add(s);
		}

		ra.feedIntegerData(integers);

		long q25 = ra.getQuantile(0.25);
		long q50 = ra.getQuantile(0.5);
		long q75 = ra.getQuantile(0.75);
		System.out.println(a.toString());
		System.out.println(q25 + " - " + q50 + " - " + q75);
	    }
	}
    }
}

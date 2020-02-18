package test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Vector;

import org.junit.Test;

import preanalysis.PreAnalyzer;
import preanalysis.Values;
import sources.deprecated.Attribute;
import sources.deprecated.Attribute.AttributeType;
import sources.implementations.CSVSource;
import sources.implementations.PostgresSource;

public class PreAnalyzerTest {

    private String path = "C:\\";
    private String filename = "Leading_Causes_of_Death__1990-2010.csv";
    // private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
    // private String filename = "short_cis_course_catalog.csv";
    private String separator = ",";
    private int numRecords = 10;
    private String db = "mysql";
    private String connIP = "localhost";
    private String port = "3306";
    private String sourceName = "/test";
    private String tableName = "nellsimple";
    private String username = "root";
    private String password = "Qatar";

    public void typeChecking(PreAnalyzer pa) {

	pa.readRows(numRecords);
	List<Attribute> attrs = pa.getEstimatedDataTypes();
	for (Attribute a : attrs) {
	    System.out.println(a);
	}

	Map<Attribute, Values> data = pa.readRows(numRecords);
	for (Entry<Attribute, Values> a : data.entrySet()) {
	    System.out.println();
	    System.out.println();
	    System.out.println();
	    System.out.println("NAME: " + a.getKey().getColumnName());
	    System.out.println("TYPE: " + a.getKey().getColumnType());
	    if (a.getKey().getColumnType().equals(AttributeType.FLOAT)) {
		List<Float> objs = a.getValue().getFloats();
		for (float f : objs) {
		    System.out.println(f);
		}
	    }
	    if (a.getKey().getColumnType().equals(AttributeType.STRING)) {
		List<String> objs = a.getValue().getStrings();
		for (String f : objs) {
		    System.out.println(f);
		}
	    }
	}
    }

    public void workloadTest(List<String> test_strings, PreAnalyzer pa) {
	long startTime = System.currentTimeMillis();
	for (int i = 0; i < test_strings.size(); i++) {
	    pa.isNumericalException(test_strings.get(i));
	}
	long endTime = System.currentTimeMillis();
	System.out.println("Exception method took: " + (endTime - startTime) + " milliseconds");
	System.out.println("----------------------------------------------------------------\n\n");

	System.out.println("Using Reg Exp based solution with workloads that are all numerical values");
	startTime = System.currentTimeMillis();
	for (int i = 0; i < test_strings.size(); i++) {
	    pa.isNumerical(test_strings.get(i));
	}
	endTime = System.currentTimeMillis();
	System.out.println("Reg Exp based method took: " + (endTime - startTime) + " milliseconds");
    }

    @Test
    public void testRegExpPerformance() {
	PreAnalyzer pa = new PreAnalyzer(null);
	final int NUM_TEST_STRINGS = 1000000;
	final double DOUBLE_RANGLE_MIN = 1.0;
	final double DOUBLE_RANGLE_MAX = 10000000.0;
	List<String> testStrings = new Vector<String>();
	double start = DOUBLE_RANGLE_MIN;
	double end = DOUBLE_RANGLE_MAX;
	Random randomSeeds = new Random();

	for (int i = 0; i < NUM_TEST_STRINGS; i++) {
	    double randomGen = randomSeeds.nextDouble();
	    double result = start + (randomGen * (end - start));
	    testStrings.add(result + "");
	}

	// testing workloads that are numberical values, in this case the
	// try-catch
	// approach will not never incur an exception
	System.out.println("Test with workloads that are all numerical values");
	workloadTest(testStrings, pa);
	System.out.println("----------------------------------------------------------------\n\n");

	for (int i = 0; i < NUM_TEST_STRINGS / 2; i++) {
	    testStrings.set(i, "A");
	}

	System.out.println("Test with workloads that half of them are numerical values");
	workloadTest(testStrings, pa);
	System.out.println("----------------------------------------------------------------\n\n");

	for (int i = NUM_TEST_STRINGS / 2; i < NUM_TEST_STRINGS; i++) {
	    testStrings.set(i, "A");
	}
	System.out.println("Test with workloads that all them are numerical values");
	workloadTest(testStrings, pa);
	System.out.println("----------------------------------------------------------------\n\n");
    }

    @Test
    public void testPreAnalyzerForTypesCSVFile() throws IOException {

	// FIXME: create config on the fly
	CSVSource fc = new CSVSource();

	PreAnalyzer pa = new PreAnalyzer(null);
	pa.assignSourceTask(fc);
	System.out.println("------------begin type checking with FileConnector");
	typeChecking(pa);
	System.out.println("------------finish type checking with FileConnector");
    }

    @Test
    public void testPreAnalyzerForTypesDB() throws IOException {

	// Old_DBConnector dbc = new Old_DBConnector("", DBType.MYSQL, connIP,
	// port, sourceName, tableName, username,
	// password);

	// FIXME: create config on the fly
	PostgresSource dbc = new PostgresSource();

	PreAnalyzer pa = new PreAnalyzer(null);
	pa.assignSourceTask(dbc);
	System.out.println("------------begin type checking with DBConnector");
	typeChecking(pa);
	System.out.println("------------finish type checking with DBConnector");
    }
}

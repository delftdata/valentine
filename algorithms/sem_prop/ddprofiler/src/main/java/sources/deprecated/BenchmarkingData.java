package sources.deprecated;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;

@Deprecated
public class BenchmarkingData {

    public List<Attribute> attributes;
    public List<Record> records;

    public boolean populateDataFromCSVFile(String path, char separator) {
	try {
	    // Create CSV reader
	    CSVReader fileReader = new CSVReader(new FileReader(path), separator);
	    // Populate attributes
	    attributes = this.getAttributes(fileReader);
	    records = this.populateRecords(fileReader);
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return true;
    }

    private List<Attribute> getAttributes(CSVReader fileReader) throws IOException {
	// assume that the first row is the attributes;
	String[] attributes = fileReader.readNext();

	List<Attribute> attrList = new ArrayList<Attribute>();
	for (int i = 0; i < attributes.length; i++) {
	    Attribute attr = new Attribute(attributes[i]);
	    attrList.add(attr);
	}
	return attrList;
    }

    private List<Record> populateRecords(CSVReader fileReader) {
	List<Record> records = new ArrayList<>();
	String[] res = null;
	try {
	    while ((res = fileReader.readNext()) != null) {
		Record rec = new Record();
		rec.setTuples(res);
		records.add(rec);
	    }
	} catch (IOException e) {

	}
	return records;
    }

    public float approxSizeOfDataInMemory() {
	Record r = records.get(0);
	List<String> tuples = r.getTuples();
	int totalBytes = 0;
	for (String t : tuples) {
	    totalBytes += t.length();
	}
	float approxMemoryBytes = totalBytes * records.size();
	float approxMemoryMB = approxMemoryBytes / 1024 / 1024;

	return approxMemoryMB;
    }

}

package core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import preanalysis.Values;
import sources.deprecated.Attribute;
import sources.deprecated.Attribute.AttributeType;
import store.Store;

public class FilterAndBatchDataIndexer implements DataIndexer {

    private Store store;
    private String dbName;
    private String path;
    private String sourceName;
    private Map<Attribute, Long> attributeIds;
    private Map<Attribute, List<String>> attributeValues;
    private Map<Attribute, Integer> attributeValueSize;
    private int indexTriggerThreshold = 1 * 1024 * 1024 * 15; // 35 MB

    public FilterAndBatchDataIndexer(Store s, String dbName, String path, String sourceName) {
	this.store = s;
	this.path = path;
	this.dbName = dbName;
	this.sourceName = sourceName;
	this.attributeIds = new HashMap<>();
	this.attributeValues = new HashMap<>();
	this.attributeValueSize = new HashMap<>();
    }

    /**
     * TODO: For now simply make sure upserts work well. In the (near) future,
     * prepare a filtering mechanism here, fixed to the context of a source that
     * removes data per column that has already been indexed. Chances are we can
     * reduce the amount of data to index a lot!
     *
     * Final version will do, per field: - filter out already seen data (data
     * that has been seen with high probability) - batch data locally until
     * there's an important amount (configurable) - then call the store (which
     * may or may not batch the request)
     *
     * For now: - create update request on the same document to include more
     * terms (all of them) - each call to indexData involves deleting, creating,
     * reindexing doc, so we want to do it as few times as possible
     */
    @Override
    public boolean indexData(String dbName, String path, Map<Attribute, Values> data) {
	for (Entry<Attribute, Values> entry : data.entrySet()) {
	    Attribute a = entry.getKey();
	    AttributeType at = a.getColumnType();

	    if (at.equals(AttributeType.STRING)) {
		String columnName = a.getColumnName();
		// Id for the attribute - computed only once
		long id = 0;
		if (!attributeIds.containsKey(a)) {
		    id = Utils.computeAttrId(dbName, sourceName, columnName);
		    attributeIds.put(a, id);
		} else {
		    id = attributeIds.get(a);
		}

		// FIXME: introduce new call -> GC memory problems
		// storeNewValuesAndMaybeTriggerIndex(id,
		// sourceName, a, entry.getValue().getStrings());

		store.indexData(id, dbName, path, sourceName, columnName, entry.getValue().getStrings());
	    }
	}
	return true;
    }

    private void storeNewValuesAndMaybeTriggerIndex(int id, String dbName, String path, String sourceName, Attribute a,
	    List<String> newValues) {
	if (!attributeValues.containsKey(a)) {
	    attributeValues.put(a, new ArrayList<>());
	    attributeValueSize.put(a, 0);
	}
	int size = computeAproxSizeOf(newValues);
	int currentSize = attributeValueSize.get(a);
	int newSize = currentSize + size;
	attributeValueSize.put(a, newSize);
	updateValues(a, newValues);
	if (newSize > indexTriggerThreshold) {
	    // Index the batch of values
	    List<String> values = attributeValues.get(a);
	    store.indexData(id, dbName, path, sourceName, a.getColumnName(), values);
	    // Clean up
	    attributeValues.put(a, new ArrayList<>());
	    attributeValueSize.put(a, 0);
	}
    }

    private void updateValues(Attribute at, List<String> values) {
	List<String> currentValues = attributeValues.get(at);
	currentValues.addAll(values);
	attributeValues.put(at, currentValues);
    }

    private int computeAproxSizeOf(List<String> values) {
	int size = 0;
	for (String s : values) {
	    size += s.length();
	}
	return size;
    }

    @Override
    public boolean flushAndClose() {
	for (Entry<Attribute, Long> entry : attributeIds.entrySet()) {
	    Attribute a = entry.getKey();
	    store.indexData(entry.getValue(), dbName, path, sourceName, a.getColumnName(), attributeValues.get(a));
	}
	return true;
    }
}

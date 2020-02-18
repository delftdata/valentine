package core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import analysis.Analysis;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import analysis.modules.Entities;
import sources.deprecated.Attribute;
import sources.deprecated.Attribute.AttributeType;

public class WorkerTaskResultHolder {

	private List<WorkerTaskResult> results;
	
	/**
	 * Used mostly for testing/profiling
	 * @return
	 */
	public static List<WorkerTaskResult> makeFakeOne() {
		List<WorkerTaskResult> rs = new ArrayList<>();
		WorkerTaskResult wtr = new WorkerTaskResult(
				-1,	"none", "none", "none",	"none",	"N", 100, 100, 0, 100, 50, 50, 50);
		rs.add(wtr);
		return rs;
	}
	
	public WorkerTaskResultHolder(List<WorkerTaskResult> results) {
		this.results = results;
	}
	
	public WorkerTaskResultHolder(String dbName, String path, String sourceName, List<Attribute> attributes, Map<String, Analysis> analyzers) {
		List<WorkerTaskResult> rs = new ArrayList<>();
		for(Attribute a : attributes) {
			AttributeType at = a.getColumnType();
			Analysis an = analyzers.get(a.getColumnName());
			long id = Utils.computeAttrId(dbName, sourceName, a.getColumnName());
			if(at.equals(AttributeType.FLOAT)) {
				NumericalAnalysis na = ((NumericalAnalysis)an);
				WorkerTaskResult wtr = new WorkerTaskResult(
						id,
						dbName,
						path,
						sourceName,
						a.getColumnName(),
						"N",
						(int)na.getCardinality().getTotalRecords(),
						(int)na.getCardinality().getUniqueElements(),
						na.getNumericalRange(AttributeType.FLOAT).getMinF(),
						na.getNumericalRange(AttributeType.FLOAT).getMaxF(),
						na.getNumericalRange(AttributeType.FLOAT).getAvg(),
						na.getNumericalRange(AttributeType.FLOAT).getMedian(),
						na.getNumericalRange(AttributeType.FLOAT).getIQR());
				rs.add(wtr);
			}
			else if(at.equals(AttributeType.INT)) {
				NumericalAnalysis na = ((NumericalAnalysis)an);
				WorkerTaskResult wtr = new WorkerTaskResult(
						id,
						dbName,
						path,
						sourceName,
						a.getColumnName(),
						"N",
						(int)na.getCardinality().getTotalRecords(),
						(int)na.getCardinality().getUniqueElements(),
						na.getNumericalRange(AttributeType.INT).getMin(),
						na.getNumericalRange(AttributeType.INT).getMax(),
						na.getNumericalRange(AttributeType.INT).getAvg(),
						na.getNumericalRange(AttributeType.INT).getMedian(),
						na.getNumericalRange(AttributeType.INT).getIQR());
				rs.add(wtr);
			}
			else if(at.equals(AttributeType.STRING)) {
				TextualAnalysis ta = ((TextualAnalysis)an);
				Entities e = ta.getEntities();
				long[] mh = ta.getMH();
				List<String> ents = e.getEntities();
				StringBuffer sb = new StringBuffer();
				for(String str : ents) {
					sb.append(str);
					sb.append(" ");
				}
				String entities = sb.toString();
				
				WorkerTaskResult wtr = new WorkerTaskResult(
						id,
						dbName,
						path,
						sourceName,
						a.getColumnName(),
						"T",
						(int)ta.getCardinality().getTotalRecords(),
						(int)ta.getCardinality().getUniqueElements(),
						entities,
						mh);
				rs.add(wtr);
			}
		}
		this.results = rs;
	}
	
	public List<WorkerTaskResult> get() {
		return results;
	}

}

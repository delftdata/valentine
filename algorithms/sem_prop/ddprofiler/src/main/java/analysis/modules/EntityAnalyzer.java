/**
 * @author Raul - raulcf@csail.mit.edu
 * @author Sibo Wang (edit)
 */
package analysis.modules;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import analysis.AnalyzerFactory;
import analysis.TextualDataConsumer;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.Span;

public class EntityAnalyzer implements TextualDataConsumer {

  private class EntityAnalyzerStatistics {
    private String nameFinderStr;
    private int
        hitCount; // the number of strings that matches a certain OpenNLP model.
    private double avgProb = 0.0; // each match in the hitCount will include a
                                  // probability to show its confidence,
    // avgProb = sum of these prob / hitCount

    private double feedTokenAvgCount =
        0.0; // each textual input will be tokenized into several words,
    // the feedTokenAvgCount = the total number of such tokened words / the
    // number of input records;

    private int feedCount =
        0; // the number of records feeded into the EntityAnalyzer

    public EntityAnalyzerStatistics(String nameFinderStr) {
      this.setNameFinderStr(nameFinderStr);
    }
    public String getNameFinderStr() { return nameFinderStr; }
    public void setNameFinderStr(String nameFinderStr) {
      this.nameFinderStr = nameFinderStr;
    }

    public int getHitCount() { return hitCount; }

    public void updateAverageProb(double prob) {
      hitCount++;
      this.avgProb = ((hitCount - 1) * this.avgProb + prob) / hitCount;
    }
    public double getAvgProb() {
      // TODO Auto-generated method stub
      return avgProb;
    }

    public void updateAvgFeedTokenCount(int incFeedTokenCount) {
      this.feedCount++;
      this.feedTokenAvgCount =
          (incFeedTokenCount + this.feedTokenAvgCount * (feedCount - 1)) /
          feedCount;
    }

    public String toString() {
      return this.nameFinderStr + " " + this.feedTokenAvgCount + " " +
          this.hitCount + " " + this.avgProb;
    }
  }

  private Set<String> entities = null;
  private List<NameFinderME> nameFinderList = null;
  private Properties prop = null;
  private Tokenizer tok = SimpleTokenizer.INSTANCE;
  private List<TokenNameFinderModel> modelList = null;
  private Map<NameFinderME, EntityAnalyzerStatistics> eaStatics;
  private List<String> modelNameList = null;
  private boolean isConverged = false;
  private EntityAnalyzerStatistics lastConvergedEAStat = null;

  public EntityAnalyzer(List<TokenNameFinderModel> modelList,
                        List<String> modelNameList) {
    prop = new Properties();
    entities = new HashSet<>();
    nameFinderList = new Vector<NameFinderME>();
    this.modelNameList = modelNameList;
    eaStatics = new HashMap<NameFinderME, EntityAnalyzerStatistics>();

    for (int i = 0; i < modelList.size(); i++) {
      NameFinderME nameFinder = new NameFinderME(modelList.get(i));
      nameFinderList.add(nameFinder);
      eaStatics.put(nameFinder,
                    new EntityAnalyzerStatistics(modelNameList.get(i)));
    }
  }

  public EntityAnalyzer() {
    prop = new Properties();
    entities = new HashSet<>();
    nameFinderList = new Vector<NameFinderME>();
    modelNameList = new Vector<String>();
    eaStatics = new HashMap<NameFinderME, EntityAnalyzerStatistics>();

    InputStream nlpModeConfigStream;
    try {
      //			nlpModeConfigStream =
      //AnalyzerFactory.class.getClassLoader().getResource(
      //					"config" + File.separator +
      //"nlpmodel.config").openStream();
      nlpModeConfigStream = AnalyzerFactory.class.getClassLoader()
                                .getResource("nlpmodel.config")
                                .openStream();
      modelList = loadModel(nlpModeConfigStream);
      System.out.println(modelList.size());

    } catch (IOException e) {
      e.printStackTrace();
    }

    for (int i = 0; i < modelList.size(); i++) {
      NameFinderME nameFinder = new NameFinderME(modelList.get(i));
      nameFinderList.add(nameFinder);
      eaStatics.put(nameFinder,
                    new EntityAnalyzerStatistics(modelNameList.get(i)));
    }
  }

  public List<TokenNameFinderModel> getCachedModelList() { return modelList; }

  public List<String> getCachedModelNameList() { return this.modelNameList; }

  public Entities getEntities() {
    Entities e = new Entities(entities);
    return e;
  }

  private String[] preprocessFeedString(String[] strArr, String modelName) {

    /*
     * current person and location model cannot recognize the entity based on
     * the string
     * unless the first char of the string is
     * upper case and the latters are lower cases.
    */

    if (modelName.equals("person") || modelName.equals("location")) {
      // first convert all chars to lower case, and then set the first to upper
      // case.
      String[] retArr = new String[strArr.length];
      for (int i = 0; i < strArr.length; i++) {
        String val = strArr[i].trim().toLowerCase();
        if (val.length() == 0) {
          retArr[i] = val;
        } else
          retArr[i] = val.substring(0, 1).toUpperCase() + val.substring(1);
      }
      return retArr;
    }

    // TODO: does other models need preprocessing?

    return strArr;
  }

  @SuppressWarnings("unused")
  private void printEAStatics() {
    for (Map.Entry<NameFinderME, EntityAnalyzerStatistics> ent :
         eaStatics.entrySet()) {
      EntityAnalyzerStatistics stat = ent.getValue();
      if (stat.avgProb > 0.0)
        System.out.println(stat);
    }
  }

  // check whether entity recognition has converged
  private boolean estimateConverged() {

    printEAStatics();

    // if the identified entity is the same as the last one, and its probability
    // is larger than 0.5
    // then we regard it as converged.

    EntityAnalyzerStatistics dominateAnalyzer = null;
    for (Map.Entry<NameFinderME, EntityAnalyzerStatistics> ent :
         eaStatics.entrySet()) {
      EntityAnalyzerStatistics eas = ent.getValue();
      if (eas.getAvgProb() > 0.0) {

        // TODO: do we compare hitCount? i.e., the number of strings that
        // matches a certain type?
        if (dominateAnalyzer != null) {
          if (dominateAnalyzer.getAvgProb() < eas.getAvgProb()) {
            dominateAnalyzer = eas;
          }
        } else {
          dominateAnalyzer = eas;
        }
      }
    }

    if (dominateAnalyzer != null && lastConvergedEAStat == null) {
      lastConvergedEAStat = dominateAnalyzer;
    }
    if (dominateAnalyzer != null && dominateAnalyzer == lastConvergedEAStat &&
        dominateAnalyzer.avgProb > 0.5) {
      // converged
      return true;
    }

    return false;
  }

  @Override
  public boolean feedTextData(List<String> records) {
    // TODO: preprocessing to records?

    //		if(isConverged){
    //			return false;
    //		}
    //
    //		String[] sentences = new String[records.size()];
    //		for (int i = 0; i < sentences.length; i++) {
    //			sentences[i] = records.get(i);
    //		}
    //		for (NameFinderME nameFinder : nameFinderList) {
    //			EntityAnalyzerStatistics eas =
    //eaStatics.get(nameFinder);
    //			for(int i=0; i<sentences.length; i++){
    //				String[] inputStr =
    //preprocessFeedString(tok.tokenize(sentences[i]),
    //						eas.getNameFinderStr()) ;
    //				eas.updateAvgFeedTokenCount(inputStr.length);
    //				Span nameSpans[] = nameFinder.find(inputStr);
    //
    //				for (Span s : nameSpans) {
    //					entities.add(s.getType());
    //					eas.updateAverageProb(s.getProb());;
    //				}
    //			}
    //
    //
    //			// This is supposed to be called temporarily to clean up
    //data
    //			nameFinder.clearAdaptiveData();
    //
    //		}
    //
    //
    //		//printEAStatics();
    //		isConverged = estimateConverged();
    //
    //		return false;

    return true;
  }

  public List<TokenNameFinderModel> loadModel(InputStream modelConfigStream) {
    /*
     * currently, we adopted the models provided by openNLP the detailed
     * models are listed in the model_list_file_name
     */
    List<TokenNameFinderModel> modelList = new Vector<TokenNameFinderModel>();
    InputStream input = null;
    try {
      input = modelConfigStream;
      prop.load(input);
      Enumeration<?> enumVar = prop.propertyNames();
      while (enumVar.hasMoreElements()) {
        String key = (String)enumVar.nextElement();
        String value = prop.getProperty(key);
        // System.out.println("Key : " + key + ", Value : " + value);

        InputStream modelIn = null;
        TokenNameFinderModel model = null;
        try {
          modelIn = new FileInputStream(value);
          model = new TokenNameFinderModel(modelIn);
          modelList.add(model);
          modelNameList.add(key);
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          if (modelIn != null) {
            try {
              modelIn.close();
            } catch (IOException e) {
            }
          }
        }
      }
    } catch (IOException e1) {
      e1.printStackTrace();
    }

    return modelList;
  }

  public void clear() {
    this.entities.clear();
    for (NameFinderME nameFinder : nameFinderList) {
      nameFinder.clearAdaptiveData();
    }
  }
}

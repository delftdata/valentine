package test;

import java.util.List;

import org.junit.Test;

import analysis.AnalyzerFactory;
import analysis.NumericalAnalysis;
import analysis.modules.Cardinality;
import analysis.modules.Range;
import sources.deprecated.Attribute.AttributeType;
import utils.NumericalColumnGenerator;
import utils.NumericalColumnGenerator.Distribution;

public class NumericalEstimatorTest {
  public final int RANDOM_SEQ_LENGTH = 100000;

  public void loadInput(List<Float> randomList) {
    System.out.println("True 25 percentile: " +
                       randomList.get(RANDOM_SEQ_LENGTH * 25 / 100));
    System.out.println("True 50 percentile: " +
                       randomList.get(RANDOM_SEQ_LENGTH * 50 / 100));
    System.out.println("True 75 percentile: " +
                       randomList.get(RANDOM_SEQ_LENGTH * 75 / 100));

    NumericalAnalysis na = AnalyzerFactory.makeNumericalAnalyzer();
    na.feedFloatData(randomList);
    Cardinality c = na.getCardinality();
    Range r = na.getNumericalRange(AttributeType.FLOAT);

    System.out.println("Cardinality:");
    System.out.println(c);
    System.out.println("Range:");
    System.out.println(r);
    System.out.println("Signature:");
    System.out.println("25 percentile: " + na.getQuantile(0.25));
    System.out.println("50 percentile: " + na.getQuantile(0.5));
    System.out.println("75 percentile: " + na.getQuantile(0.75));
  }

  @Test
  public void testGaussianInput() {
    System.out.println(
        "------------------begining of Gaussian workload test---------------------");
    NumericalColumnGenerator ncg =
        new NumericalColumnGenerator(Distribution.GAUSSIAN);
    List<Float> randomList =
        ncg.generateRandomSequence(175, 15, RANDOM_SEQ_LENGTH);
    randomList.sort(null);
    int uniqueElements = 1;
    float lastElement = randomList.get(0);
    for (int i = 1; i < randomList.size(); i++) {
      float currElement = randomList.get(i);
      if (lastElement == currElement) {
        lastElement = currElement;
        continue;
      }
      uniqueElements++;
      lastElement = currElement;
    }
    System.out.println("Cardinality:" + uniqueElements);
    loadInput(randomList);
    System.out.println(
        "------------------end of Gaussian workload test---------------------");
  }

  @Test
  public void testUniformInput() {
    System.out.println(
        "------------------begining of Uniform workload test---------------------");

    NumericalColumnGenerator ncg =
        new NumericalColumnGenerator(Distribution.UNIFORM);
    List<Float> randomList =
        ncg.generateRandomSequence(175, 15, RANDOM_SEQ_LENGTH);
    randomList.sort(null);
    int uniqueElements = 1;
    float lastElement = randomList.get(0);
    for (int i = 1; i < randomList.size(); i++) {
      float currElement = randomList.get(i);
      if (lastElement == currElement) {
        lastElement = currElement;
        continue;
      }
      uniqueElements++;
      lastElement = currElement;
    }
    System.out.println("Cardinality:" + uniqueElements);
    loadInput(randomList);
    System.out.println(
        "------------------end of Uniform workload test---------------------");
  }
}

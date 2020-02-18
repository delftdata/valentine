package utils;

import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.junit.Test;

public class NumericalColumnGenerator {

  private Distribution dist;
  private Random random;
  public NumericalColumnGenerator(Distribution dist) {
    this.dist = dist;
    random = new Random(System.currentTimeMillis());
  }

  public static enum Distribution { GAUSSIAN, UNIFORM }
  private final double sqrtThree = Math.sqrt(3);
  public List<Float> generateRandomSequence(double mean, double stdDeviation,
                                            int randomLength) {
    List<Float> floatList = new Vector<Float>();
    if (dist == Distribution.UNIFORM) {
      for (int i = 0; i < randomLength; i++) {
        Float randFloat =
            (float)(mean + 2 * sqrtThree * random.nextDouble() * stdDeviation);
        floatList.add(randFloat);
      }
    } else {
      for (int i = 0; i < randomLength; i++) {
        Float randFloat = (float)(mean + random.nextGaussian() * stdDeviation);
        floatList.add(randFloat);
      }
    }
    return floatList;
  }
}

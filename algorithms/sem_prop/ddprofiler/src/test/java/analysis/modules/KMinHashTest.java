package analysis.modules;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import core.MurmurHash3;

public class KMinHashTest {

    private static long hash(String string) {
	long h = (1 << 61) - 1; // prime
	int len = string.length();
	for (int i = 0; i < len; i++) {
	    h = 31 * h + string.charAt(i);
	}
	return h;
    }

    private static long hash2(String string) {
	int h = MurmurHash3.murmurhash3_x86_32(string, 0, string.length(), 1);
	return h;
    }

    public static void buildSketch(int K, int iterations, List<String> records) {
	// fixed variables
	long MERSENNE_PRIME = (1 << 61) - 1;
	long[] minhash;
	long[][] rndSeeds;

	int pseudoRandomSeed = 1;

	minhash = new long[K];
	rndSeeds = new long[K][2];
	Random rnd = new Random(pseudoRandomSeed);

	for (int i = 0; i < minhash.length; i++) {
	    minhash[i] = Long.MAX_VALUE;
	    long nextSeed = rnd.nextLong();
	    rndSeeds[i][0] = nextSeed;
	    nextSeed = rnd.nextLong();
	    rndSeeds[i][1] = nextSeed;
	}

	while (iterations > 0) {
	    iterations--;
	    for (String r : records) {
		long rawHash = hash(r);
		for (int i = 0; i < K; i++) {
		    // h = (a * x) + b
		    long hash = (rndSeeds[i][0] * rawHash + rndSeeds[i][1]) % MERSENNE_PRIME;
		    if (hash < minhash[i]) {
			minhash[i] = hash;
		    }
		}

	    }
	}
    }

    public static void statisticallySignificant(int K, int iterations, List<String> records) {
	System.out.println("K=" + K + "; iterations=" + iterations + "; ");
	int reps = 100;
	long[] all_results = new long[reps];
	for (int i = 0; i < reps; i++) {
	    long time_start = System.currentTimeMillis();
	    buildSketch(K, iterations, records);
	    long time_end = System.currentTimeMillis();
	    long total = time_end - time_start;
	    // System.out.println(total);
	    all_results[i] = total;
	}
	long total = 0;
	for (long i : all_results) {
	    total += i;
	}
	long avg = total / reps;

	long numerator = 0;
	for (long i : all_results) {
	    float val = (float) Math.pow(i - avg, 2);
	    numerator += val;
	}

	float std = (float) Math.sqrt((numerator / (reps - 1)));

	System.out.println("Avg time: " + avg);
	System.out.println("Std: " + std);

    }

    public static void main(String args[]) {

	// Create a column
	int colSize = 1000;
	List<String> records = new ArrayList<>();
	for (int i = 0; i < colSize; i++) {
	    String r = "" + i;
	    records.add(r);
	}

	int iterations = 1000;
	System.out.println("Building sketches...");

	int K = 512;
	statisticallySignificant(K, iterations, records);
	System.out.println("");

	K = 256;
	statisticallySignificant(K, iterations, records);
	System.out.println("");

	K = 128;
	statisticallySignificant(K, iterations, records);
	System.out.println("");

	K = 64;
	statisticallySignificant(K, iterations, records);
	System.out.println("");

	K = 1;
	statisticallySignificant(K, iterations, records);
	System.out.println("");
	System.out.println("Building sketches...OK");

    }

}

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import analysis.TextualDataConsumer;

public class KMinHash implements TextualDataConsumer {

    private int K = 512; // default
    final private long MERSENNE_PRIME = (1 << 61) - 1;
    private long[] minhash;
    private long[][] rndSeeds;

    public long getMersennePrime() {
	return MERSENNE_PRIME;
    }

    public long[][] getRndSeeds() {
	return rndSeeds;
    }

    public KMinHash(int pseudoRandomSeed, int K) {
	this.K = K;
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
    }

    public KMinHash(int pseudoRandomSeed) {
	this.K = 512;
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
    }

    private static long hash(String string) {

	long h = (1 << 61) - 1; // prime
	int len = string.length();

	for (int i = 0; i < len; i++) {
	    h = 31 * h + string.charAt(i);
	}
	return h;
    }

    @Override
    public boolean feedTextData(List<String> records) {
	for (String r : records) {
	    r = r.replace("_", " ");
	    r = r.replace("-", " ");
	    String[] tokens = r.split(" ");
	    for (String token : tokens) {
		token = token.toLowerCase();
		long rawHash = hash(token);
		for (int i = 0; i < K; i++) {
		    // h = (a * x) + b
		    long hash = (rndSeeds[i][0] * rawHash + rndSeeds[i][1]) % MERSENNE_PRIME;
		    if (hash < minhash[i]) {
			minhash[i] = hash;
		    }
		}
	    }
	}

	return true;
    }

    public long[] getMH() {
	return minhash;
    }

    public static void main(String args[]) {
	int pseudoRandomSeed = 1;
	KMinHash mh = new KMinHash(pseudoRandomSeed);
	List<String> records = new ArrayList<>();
	records.add("test");
	records.add("test1");
	records.add("torpedo");
	records.add("raiz");
	records.add("agua");
	records.add("water");
	mh.feedTextData(records);

	System.out.println(Long.MAX_VALUE);
	System.out.println("MERSENNE PRIME: " + mh.getMersennePrime());
	long seeds[][] = mh.getRndSeeds();
	for (int i = 0; i < seeds.length; i++) {
	    System.out.println("SEED0: " + seeds[i][0]);
	    System.out.println("SEED1: " + seeds[i][1]);
	}

	long[] minhash = mh.getMH();
	for (int i = 0; i < minhash.length; i++) {
	    System.out.println(minhash[i]);
	}

    }

}
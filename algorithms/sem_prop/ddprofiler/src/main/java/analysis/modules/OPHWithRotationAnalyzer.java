/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.util.List;

import analysis.TextualDataConsumer;
import core.MurmurHash3;

public class OPHWithRotationAnalyzer implements TextualDataConsumer {
	
	final private int K = 512;
	final private int SEED = 666; // Not caring about security for now
	private int[] minhash;
	private int[] emptyBuckets;
	
	public OPHWithRotationAnalyzer() {
		minhash = new int[K];
		emptyBuckets = new int[K];
		for(int i = 0; i < minhash.length; i++) {
			minhash[i] = Integer.MAX_VALUE;
			emptyBuckets[i] = 0; // 0 means empty
		}
	}


	@Override
	public boolean feedTextData(List<String> records) {

		for (String r : records) {
			
			int hash = MurmurHash3.murmurhash3_x86_32(r, 0, r.length(), SEED);
			int bucket = hash % K;
			if (hash < minhash[bucket]) {
				minhash[bucket] = hash;
				minhash[bucket] = 1; // 1 means non-empty
			}
			
		}
    

		return true;
	}
	
	
}
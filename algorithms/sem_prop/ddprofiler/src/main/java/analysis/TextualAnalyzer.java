/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import analysis.modules.Cardinality;
import analysis.modules.CardinalityAnalyzer;
import analysis.modules.Entities;
import analysis.modules.EntityAnalyzer;
import analysis.modules.KMinHash;

public class TextualAnalyzer implements TextualAnalysis {

    private List<DataConsumer> analyzers;
    private CardinalityAnalyzer ca;
    private KMinHash mh;
    private EntityAnalyzer ea;

    private TextualAnalyzer(EntityAnalyzer ea, int pseudoRandomSeed) {
	analyzers = new ArrayList<>();
	mh = new KMinHash(pseudoRandomSeed);
	ca = new CardinalityAnalyzer();
	this.ea = ea;
	analyzers.add(ca);
	analyzers.add(mh);
	analyzers.add(ea);
    }

    public static TextualAnalyzer makeAnalyzer(EntityAnalyzer ea2, int pseudoRandomSeed) {
	ea2.clear();
	return new TextualAnalyzer(ea2, pseudoRandomSeed);
    }

    @Override
    public boolean feedTextData(List<String> records) {
	Iterator<DataConsumer> dcs = analyzers.iterator();
	while (dcs.hasNext()) {
	    TextualDataConsumer dc = (TextualDataConsumer) dcs.next();
	    dc.feedTextData(records);
	}

	return false;
    }

    @Override
    public DataProfile getProfile() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Cardinality getCardinality() {
	return ca.getCardinality();
    }

    @Override
    public Entities getEntities() {
	return ea.getEntities();
    }

    @Override
    public long[] getMH() {
	return mh.getMH();
    }

}

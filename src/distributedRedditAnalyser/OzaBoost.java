/*
 *    OzaBoost.java
 *    @author Richard Kirkby (rkirkby@cs.waikato.ac.nz)
 *    @author Luke Barnett (luke@barnett.net.nz)
 *
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program. If not, see <http://www.gnu.org/licenses/>.
 *    
 */
package distributedRedditAnalyser;

import java.util.ArrayDeque;
import java.util.Random;
import java.util.concurrent.Semaphore;

import weka.core.Instance;
import moa.classifiers.AbstractClassifier;
import moa.classifiers.Classifier;
import moa.core.DoubleVector;
import moa.core.Measurement;
import moa.core.MiscUtils;
import moa.options.ClassOption;
import moa.options.FlagOption;
import moa.options.IntOption;

/**
 * Rewrite of the moa implementation of OzaBoost to accommodate a distributed setting and the sharing of classifiers
 * 
 * Keeps the latest K classifiers
 * 
 * Largely derrived from the moa implementation:
 * http://code.google.com/p/moa/source/browse/moa/src/main/java/moa/classifiers/meta/OzaBoost.java
 * 
 * @author Luke Barnett 1109967
 *
 */
public class OzaBoost extends AbstractClassifier {
	private static final long serialVersionUID = -4456874021287021340L;
	
	private Semaphore lock = new Semaphore(1);
	
	@Override
    public String getPurposeString() {
        return "Incremental on-line boosting of Oza and Russell.";
    }

    public ClassOption baseLearnerOption = new ClassOption("baseLearner", 'l',
            "Classifier to train.", Classifier.class, "trees.HoeffdingTree");

    public IntOption ensembleSizeOption = new IntOption("ensembleSize", 's',
            "The max number of models to boost.", 10, 1, Integer.MAX_VALUE);

    public FlagOption pureBoostOption = new FlagOption("pureBoost", 'p',
            "Boost with weights only; no poisson.");

    protected ArrayDeque<ClassifierInstance> ensemble;

    @Override
    public void resetLearningImpl() {
    	try {
			lock.acquire();		
	        this.ensemble = new ArrayDeque<ClassifierInstance>(ensembleSizeOption.getValue());
	        Classifier baseLearner = (Classifier) getPreparedClassOption(this.baseLearnerOption);
	        baseLearner.resetLearning();
    	} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			lock.release();
		}
    }

    @Override
    public void trainOnInstanceImpl(Instance inst) {
    	try {
			lock.acquire();
			//Get a new classifier
	    	Classifier newClassifier = ((Classifier) getPreparedClassOption(this.baseLearnerOption)).copy();
	    	ensemble.add(new ClassifierInstance(newClassifier));
	    	
	    	//If we have too many classifiers
	    	while(ensemble.size() > ensembleSizeOption.getValue())
	    		ensemble.pollFirst();
	    	
	        double lambda_d = 1.0;
	        for(ClassifierInstance c : ensemble){
	        	double k = this.pureBoostOption.isSet() ? lambda_d : MiscUtils.poisson(lambda_d, this.classifierRandom);
	        	if (k > 0.0) {
	                Instance weightedInst = (Instance) inst.copy();
	                weightedInst.setWeight(inst.weight() * k);
	                c.getClassifier().trainOnInstance(weightedInst);
	            }
	            if (c.getClassifier().correctlyClassifies(inst)) {
	                c.setScms(c.getScms() + lambda_d);
	                lambda_d *= this.trainingWeightSeenByModel / (2 * c.getScms());
	            } else {
	            	c.setSwms(c.getSwms() + lambda_d);
	                lambda_d *= this.trainingWeightSeenByModel / (2 * c.getSwms());
	            }
	        }
    	} catch (InterruptedException e) {
			e.printStackTrace();
		}finally{
			lock.release();
		}
    }

    protected double getEnsembleMemberWeight(ClassifierInstance i) {
        double em = i.getSwms() / (i.getScms() + i.getSwms());
        if ((em == 0.0) || (em > 0.5)) {
            return 0.0;
        }
        double Bm = em / (1.0 - em);
        return Math.log(1.0 / Bm);
    }

    public double[] getVotesForInstance(Instance inst) {
    	DoubleVector combinedVote = new DoubleVector();
    	try {
			lock.acquire();
	        for(ClassifierInstance c : ensemble){
	        	double memberWeight = getEnsembleMemberWeight(c);
	        	if (memberWeight > 0.0) {
	                DoubleVector vote = new DoubleVector(c.getClassifier().getVotesForInstance(inst));
	                if (vote.sumOfValues() > 0.0) {
	                    vote.normalize();
	                    vote.scaleValues(memberWeight);
	                    combinedVote.addValues(vote);
	                }
	            } else {
	                break;
	            }
	        }
    	} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			lock.release();
		}
        return combinedVote.getArrayRef();
    }

    public boolean isRandomizable() {
        return true;
    }

    @Override
    public void getModelDescription(StringBuilder out, int indent) {}

    @Override
    protected Measurement[] getModelMeasurementsImpl() {
        return new Measurement[]{new Measurement("ensemble size",
                    this.ensemble != null ? this.ensemble.size() : 0)};
    }

    @Override
    public Classifier[] getSubClassifiers() {
    	Classifier[] classifiers = new Classifier[ensemble.size()];
    	try {
			lock.acquire();
	    	int i = 0;
	    	for(ClassifierInstance c : ensemble){
	    		if(i < classifiers.length){
	    			classifiers[i] = c.getClassifier().copy();
	    		}else{
	    			break;
	    		}
	    		i++;
	    	}
    	} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			lock.release();
		}
        return classifiers;
    }
    
    public ClassifierInstance getLatestClassifier(){
    	return ensemble.peekLast();
    }

    public void addClassifier(ClassifierInstance c){
    	try {
			lock.acquire();
	    	ensemble.add(c.clone());
	    	
	    	//If we have too many classifiers
	    	while(ensemble.size() > ensembleSizeOption.getValue())
	    		ensemble.pollFirst();
    	
    	} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			lock.release();
		}
    }
}

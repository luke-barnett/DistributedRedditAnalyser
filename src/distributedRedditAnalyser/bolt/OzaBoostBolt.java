package distributedRedditAnalyser.bolt;

import java.util.Map;

import weka.core.Instances;
import weka.core.SparseInstance;

import moa.classifiers.Classifier;
import moa.classifiers.meta.OzaBoost;
import moa.core.InstancesHeader;
import moa.options.ClassOption;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OzaBoostBolt extends BaseRichBolt {
	
	private final OzaBoost classifier;
	private OutputCollector collector;
	private InstancesHeader INST_HEADERS;

	private static final long serialVersionUID = 5699756297412652215L;
	
	public OzaBoostBolt(String classifierName){
		classifier = new OzaBoost();
		//TODO Set the base learner + other settings
		classifier.baseLearnerOption = new ClassOption("baseLearner", 'l',"Classifier to train.",Classifier.class, classifierName);
		classifier.prepareForUse();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		
		Object obj = input.getValue(0);
		
		if(obj.getClass() == Instances.class){
			INST_HEADERS = new InstancesHeader((Instances) obj);
			classifier.setModelContext(INST_HEADERS);
			classifier.resetLearningImpl();
		}else{
			SparseInstance inst = (SparseInstance) obj;
			//Emit the predicted value and the correct value
			collector.emit(new Values(classifier.correctlyClassifies(inst), inst.classValue()));
			//Train on instance
			classifier.trainOnInstanceImpl(inst);
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("correctlyPredicted","actualClass"));
	}

}

package distributedRedditAnalyser.bolt;

import java.util.Map;

import weka.core.Instance;

import moa.classifiers.meta.OzaBoost;

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

	private static final long serialVersionUID = 5699756297412652215L;
	
	public OzaBoostBolt(){
		classifier = new OzaBoost();
		//TODO Set the base learner + other settings
		//classifier.baseLearnerOption = null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Instance inst = (Instance) input;
		
		//Emit the predicted value and the correct value
		collector.emit(new Values(classifier.getVotesForInstance(inst), inst.classValue()));
		collector.ack(input);
		
		//Train on instance
		classifier.trainOnInstanceImpl(inst);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("predictedClass","actualClass"));
	}

}

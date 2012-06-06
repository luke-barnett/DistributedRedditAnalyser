package distributedRedditAnalyser.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import distributedRedditAnalyser.reddit.Post;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 
 * bolt that collects stream tuples and turns them into an instance object
 * so they are can be processed by weka and moa classes
 * 
 * @author Tony Chen 1111377
 * @author Luke Barnett 1109967
 * 
 */
public class InstanceBolt extends BaseRichBolt {

	private static final long serialVersionUID = -290300387298901098L;
	private OutputCollector _collector;
	private Instances instHeaders;
	private final List<String> NOMINAL_CLASS_VALUES;
	
	public InstanceBolt(List<String> nominalClassValues){
		NOMINAL_CLASS_VALUES = nominalClassValues;
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("redditInstance"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		ArrayList<Attribute> att = new ArrayList<Attribute>();
		att.add(new Attribute("title", (List<String>)null, 0));
		att.add(new Attribute("redditClass", NOMINAL_CLASS_VALUES, 1));
		instHeaders = new Instances("reddit",att, 10);
		instHeaders.setClassIndex(1);
	}

	@Override
	public void execute(Tuple input) {
		Post newPost = (Post)input.getValue(0);
		Instance inst = new DenseInstance(2);
		inst.setDataset(instHeaders);
		inst.setValue(0, newPost.getTitle());
		inst.setValue(1, newPost.getSubReddit());
		//emit these to a new bolt that collects instances
		_collector.emit(new Values(inst));
	}

}

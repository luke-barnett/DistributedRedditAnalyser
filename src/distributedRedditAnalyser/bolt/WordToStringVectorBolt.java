package distributedRedditAnalyser.bolt;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import weka.core.Instance;
import weka.filters.unsupervised.attribute.StringToWordVector;

import distributedRedditAnalyser.reddit.Post;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Takes string instances and turns them into word vectors
 * @author Luke Barnett 1109967
 *
 */
public class WordToStringVectorBolt extends BaseRichBolt{

	private static final long serialVersionUID = -7494062164103601417L;
	private final int BATCH_SIZE;
	private final int MAX_NUMBER_OF_WORDS_TO_KEEP;
	private final ArrayBlockingQueue<Instance> BATCH_QUEUE;
	private OutputCollector collector;
	
	public WordToStringVectorBolt(int batchSize, int maxNumberOfWordsToKeep){
		if(batchSize < 1){
			throw new IllegalArgumentException("Batch Size is less than 1");
		}
		if(maxNumberOfWordsToKeep < 1){
			throw new IllegalArgumentException("Max number of words to keep is less than 1");
		}
		
		MAX_NUMBER_OF_WORDS_TO_KEEP = maxNumberOfWordsToKeep;
		BATCH_SIZE = batchSize;
		BATCH_QUEUE = new ArrayBlockingQueue<Instance>(BATCH_SIZE);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("StringVectors"));
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		BATCH_QUEUE.add((Instance) input);
		collector.ack(input);
		
		if(BATCH_QUEUE.size() == BATCH_SIZE){
			//Once the queue is of size create the vector
			StringToWordVector vectorCreator = new StringToWordVector(MAX_NUMBER_OF_WORDS_TO_KEEP);
			//TODO DO THINGS
			
			//Empty Queue
			BATCH_QUEUE.clear();
		}
	}

}

package distributedRedditAnalyser.bolt;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;

import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.StringToWordVector;

import distributedRedditAnalyser.reddit.Post;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Takes string instances and turns them into word vectors
 * @author Luke Barnett 1109967
 * @author tony
 *
 */
public class StringToWordVectorBolt extends BaseRichBolt{

	private static final long serialVersionUID = -7494062164103601417L;
	private final int BATCH_SIZE;
	private final int MAX_NUMBER_OF_WORDS_TO_KEEP;
	private Instances INST_HEADERS;
	private final ArrayBlockingQueue<Instance> BATCH_QUEUE;
	private OutputCollector collector;
	private Semaphore semaphore;
	private StringToWordVector filter;
	private Boolean training = true;
	public StringToWordVectorBolt(int batchSize, int maxNumberOfWordsToKeep, Instances instHeaders){
		if(batchSize < 1){
			throw new IllegalArgumentException("Batch Size is less than 1");
		}
		if(maxNumberOfWordsToKeep < 1){
			throw new IllegalArgumentException("Max number of words to keep is less than 1");
		}
		
		MAX_NUMBER_OF_WORDS_TO_KEEP = maxNumberOfWordsToKeep;
		BATCH_SIZE = batchSize;
		BATCH_QUEUE = new ArrayBlockingQueue<Instance>(BATCH_SIZE);
		INST_HEADERS = instHeaders;
		semaphore = new Semaphore(1);
		filter = new StringToWordVector(MAX_NUMBER_OF_WORDS_TO_KEEP);
		filter.setOutputWordCounts(true);
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
		DenseInstance inst = (DenseInstance) input.getValue(0);
		INST_HEADERS = inst.dataset();
		
		try {
			semaphore.acquire();
		} catch (InterruptedException e2) {
			e2.printStackTrace();
		}
		
		if(training){
			try{
				BATCH_QUEUE.add(inst);
			}catch(IllegalStateException e){
				//Queue is full so we should train the filter
				try {
					
					Instances data = new Instances(INST_HEADERS);
					
					for(Instance i : BATCH_QUEUE){
						data.add(i);
					}
					
					filter.setInputFormat(data);
					
					//emit the instances used to train the filter
					Instances filter_training_set = Filter.useFilter(data, filter);
					for(int i=0; i<filter_training_set.numInstances(); i++){
						collector.emit(new Values(filter_training_set.get(i)));
					}
					
					training = false;
					//Empty the queue for memories
					BATCH_QUEUE.clear();
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				
			}
		}else{
			try {
				filter.input(inst);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			Instance filteredValue;
			while((filteredValue = filter.output()) != null){
				collector.emit(new Values(filteredValue));
			}
		}
		
		collector.ack(input);
		semaphore.release();
	}

}

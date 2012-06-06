package distributedRedditAnalyser.bolt;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;

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
 *
 */
public class WordToStringVectorBolt extends BaseRichBolt{

	private static final long serialVersionUID = -7494062164103601417L;
	private final int BATCH_SIZE;
	private final int MAX_NUMBER_OF_WORDS_TO_KEEP;
	private final Instances INST_HEADERS;
	private final ArrayBlockingQueue<Instance> BATCH_QUEUE;
	private OutputCollector collector;
	private Semaphore semaphore;
	private Filter filter;
	private Boolean training = true;
	
	public WordToStringVectorBolt(int batchSize, int maxNumberOfWordsToKeep, Instances instHeaders){
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
		Instance inst = (Instance) input;
		collector.ack(input);
		
		try {
			semaphore.acquire();
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
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
					
					/*
					 * TODO 
					 * I think this is needed information and that we should emit the instances?
					 * Isn't it the filtered values of the training data? Which we should be using?
					 * Maybe the output() loop will fix that anyway?
					 */
					
					/*Instances filteredData = */
					Filter.useFilter(data, filter);
					
					/*for(Instance i : filteredData){
						collector.emit(new Values(i));
					}*/
					
					filter.input(inst);
					
					Instance filteredValue;
					while((filteredValue = filter.output()) != null){
						collector.emit(new Values(filteredValue));
					}
					
					training = false;
					//Empty the queue for memories
					BATCH_QUEUE.clear();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
			}
		}else{
			try {
				filter.input(inst);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			Instance filteredValue;
			while((filteredValue = filter.output()) != null){
				collector.emit(new Values(filteredValue));
			}
		}
		
		
		
		semaphore.release();
		
	}

}

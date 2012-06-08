package distributedRedditAnalyser.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StatisticsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1283116164348536110L;
	private OutputCollector collector;
	private final int REPORTING_FREQUENCY;
	private final int NUMBER_OF_CLASSES;
	private long totalCount;
	private long totalPredictedCorrectly;
	
	public StatisticsBolt(int numberOfClasses, int reportingFrequency){
		REPORTING_FREQUENCY = reportingFrequency;
		NUMBER_OF_CLASSES = numberOfClasses;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if(input.getBoolean(0)){
			totalPredictedCorrectly++;
		}
		totalCount++;
		collector.ack(input);
		
		if(totalCount % REPORTING_FREQUENCY == 0){
			double accuracy = (double)totalPredictedCorrectly / (double)totalCount;
			//Can we assume this? Or do we need to record it?
			double randomGuessAccuracy = 1 / (double) NUMBER_OF_CLASSES;
			double kappa = (accuracy - randomGuessAccuracy) / (1 - randomGuessAccuracy);
			collector.emit(new Values(totalCount,accuracy, kappa));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("NumberSeen", "Accuracy","Kappa"));
	}

}

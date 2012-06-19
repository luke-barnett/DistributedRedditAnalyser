package distributedRedditAnalyser.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * keeps track of statistics needed to compute frequency and kappa
 * @author tony
 *
 */
public class StatisticsBolt extends BaseRichBolt {

	private static final long serialVersionUID = -3382457336041546604L;
	private OutputCollector collector;
	private final int REPORTING_FREQUENCY;
	private final int NUMBER_OF_CLASSES;
	private long totalCount;
	private long totalPredictedCorrectly;
	
	//array to keep statistics needed for kappa and accuracy
	//[0] are the actual class counts
	//[1] are the predicted class counts
	private double[][] stats;
	
	public StatisticsBolt(int numberOfClasses, int reportingFrequency){
		REPORTING_FREQUENCY = reportingFrequency;
		NUMBER_OF_CLASSES = numberOfClasses;
		stats = new double[2][numberOfClasses];	
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		
		if(input.getValue(0).getClass() == double[].class){
			double[] dist = (double[]) input.getValue(0);
			//wont cast Double objects to int, doing it the long way
			Double d_actual =(Double)input.getValue(1);
			int actual = d_actual.intValue();
			int pred = 0;
			double max = -1;
			for(int i=0;i<dist.length;i++){
				if(dist[i]>max){
					pred = i;
					max = dist[i];
				}
			}
			
			//update counted statistics
			if(pred == actual){
				totalPredictedCorrectly++;
			}
			stats[0][actual]++;
			stats[1][pred]++;	
			
			totalCount++;
			collector.ack(input);
			
			if(totalCount % REPORTING_FREQUENCY == 0){
				double accuracy = (double)totalPredictedCorrectly / (double)totalCount;
				//calculate probably of getting correct prediction by chance
				double randomGuessAccuracy = 0;
				for(int i=0;i<stats[0].length;i++){
					randomGuessAccuracy += (stats[0][i]/totalCount)*(stats[1][i]/totalCount);
				}
				double kappa = (accuracy - randomGuessAccuracy) / (1 - randomGuessAccuracy);
				collector.emit(new Values(totalCount,accuracy, Double.isNaN(kappa) ? 0 : kappa));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("NumberSeen", "Accuracy","Kappa"));
	}

}

package distributedRedditAnalyser.bolt;

import java.text.DecimalFormat;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Bolt to print statistics in a nice formatted way to standard output
 * @author Luke Barnett 1109967
 * @author Tony Chen 1111377
 *
 */
public class StatsPrinterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -2245664448752984871L;
	
	private DecimalFormat df = new DecimalFormat("#.##");
	private String name = "";
	
	public StatsPrinterBolt(String name){
		this.name = name;
	}
	
	public StatsPrinterBolt(){}

	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
		Long n = tuple.getLong(0);
		Double a = tuple.getDouble(1);
		Double k = tuple.getDouble(2);
		//Print to standard output
        System.out.println("[" + name + "] n:" + n + "\taccuracy:" + df.format(a) + "\tkappa:" + df.format(k) + "\t" + System.currentTimeMillis());
        //tuples are ack in the super class so we don't need to worry about it
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}
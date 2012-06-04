package distributedRedditAnalyser.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Basic storm bolt that prints directly to System.out
 * 
 * Taken directly from: https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/bolt/PrinterBolt.java
 *
 * @author Nathan Marz
 */
public class PrinterBolt extends BaseBasicBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}
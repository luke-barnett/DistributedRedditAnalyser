package distributedRedditAnalyser.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RawRedditSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private final String SUBREDDIT;

	/**
	 * Creates a new raw reddit spout for the provided sub-reddit
	 * @param subReddit The sub-reddit to use for the spout
	 */
	public RawRedditSpout(String subReddit){
		SUBREDDIT = subReddit;
	}

	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		//TODO: Currently only spouts out the subreddit that the post comes from need to poll reddit and emit the posts it finds
		Utils.sleep(50);
		collector.emit(new Values(SUBREDDIT));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("redditPost"));
	}

}

package distributedRedditAnalyser.spout;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import distributedRedditAnalyser.reddit.Post;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RawRedditSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private Boolean initialPull = true;
	
	private final String SUBREDDIT;
	private final String URL;
	private final String LATEST = "limit=1";
	private final ArrayBlockingQueue<Post> QUEUE;

	/**
	 * Creates a new raw reddit spout for the provided sub-reddit
	 * @param subReddit The sub-reddit to use for the spout
	 */
	public RawRedditSpout(String subReddit){
		SUBREDDIT = subReddit;
		URL = "http://www.reddit.com/r/" + SUBREDDIT + "/new/.json";
		QUEUE = new ArrayBlockingQueue<Post>(10000);
	}

	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Utils.sleep(50);
		Post nextPost = getNextPost();
		if(nextPost != null)
			collector.emit(new Values(nextPost));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("redditPost"));
	}
	
	private Post getNextPost(){
		//TODO: Have it start pulling from reddit, and build a backlog on initial fetch
		
		if(QUEUE.size() < 1){
			HttpClient httpClient = new DefaultHttpClient();
			try {
				
				if(initialPull){
					
					initialPull = false;
				}else{
					//Rate limit for the API
					Utils.sleep(5000);
					HttpGet getRequest = new HttpGet(URL + "?" + LATEST);
					ResponseHandler<String> responseHandler = new BasicResponseHandler();
		            
					String responseBody = httpClient.execute(getRequest, responseHandler);
					
					JSONParser parser= new JSONParser();
					
					JSONObject object = (JSONObject) parser.parse(responseBody);
					
					JSONObject data = (JSONObject) object.get("data");
					
					JSONArray children = (JSONArray) data.get("children");
					
					QUEUE.add(new Post((String) ((JSONObject)((JSONObject)children.get(0)).get("data")).get("title"), SUBREDDIT));
				}
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				httpClient.getConnectionManager().shutdown();
			}
		}
		
		return QUEUE.poll();
	}
	
	@Override
    public void close() {
        
    }

}

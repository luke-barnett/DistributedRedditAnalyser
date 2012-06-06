package distributedRedditAnalyser.spout;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.http.HttpVersion;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import distributedRedditAnalyser.reddit.Post;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Simply scrapes the new pages, starting from the newest going back
 * keep scraping until we shutdown, used for creating arrf files
 * to simulate a stream situation if we cannot use the stream directly
 * 
 * @author Tony Chen 1111377
 *
 */
public class SimRedditSpout extends BaseRichSpout{

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	
	private final String SUBREDDIT;
	private final String URL;
	private final ArrayBlockingQueue<Post> QUEUE;
	private final int POSTS_PER_PAGE = 25;
	private int count = 0;
	String lastItemId = "";
	
	public SimRedditSpout(String subReddit){
		SUBREDDIT = subReddit;
		URL = "http://www.reddit.com/r/" + SUBREDDIT + "/new/.json?sort=new&limit=" + POSTS_PER_PAGE;
		QUEUE = new ArrayBlockingQueue<Post>(10000);
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		Post nextPost = getNextPost();
		if(nextPost != null)
			collector.emit(new Values(nextPost));
		
	}

	private Post getNextPost() {
		if(QUEUE.size() < 1){
			HttpParams parameters = new BasicHttpParams();
			
			parameters.setParameter(CoreProtocolPNames.PROTOCOL_VERSION, HttpVersion.HTTP_1_1);
			parameters.setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET, "ISO-8859-1");
			parameters.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true);
			parameters.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8192);
			parameters.setParameter(CoreProtocolPNames.USER_AGENT, "DistributedRedditAnalyser /u/Technicolour/");
			
			DefaultHttpClient httpClient = new DefaultHttpClient(parameters);
			try{
				
				HttpGet getRequest = new HttpGet(URL +"&count=" + count + "&after=" + lastItemId);
				//System.out.println(URL +"&count=" + count + "&after=" + lastItemId);
				ResponseHandler<String> responseHandler = new BasicResponseHandler();
				
				String responseBody = httpClient.execute(getRequest, responseHandler);
				
				JSONParser parser= new JSONParser();
				
				JSONObject wrappingObject = (JSONObject) parser.parse(responseBody);
				
				JSONObject wrappingObjectData = (JSONObject) wrappingObject.get("data");
				
				JSONArray children = (JSONArray) wrappingObjectData.get("children");
				
				for(Object c : children){
					JSONObject childData = (JSONObject) ((JSONObject) c).get("data");
					QUEUE.add(new Post((String) childData.get("title"), SUBREDDIT));
				}
				
				lastItemId = (String) wrappingObjectData.get("after");
				
				count += POSTS_PER_PAGE;
				
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		return QUEUE.poll();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("redditPost"));
		
	}

}

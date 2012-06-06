package distributedRedditAnalyser.spout;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie2;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
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

/**
 * Directly interfaces with the Reddit API to create a raw stream of reddit posts
 * 
 * @author Luke Barnett 1109967
 * @author Tony Chen 1111377
 *
 */
public class RawRedditSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = -4867266218997902575L;
	private SpoutOutputCollector collector;
	private Boolean initialPull = true;
	private long latestTimestamp = Long.MIN_VALUE;
	
	private final String SUBREDDIT;
	private final String URL;
	private final ArrayBlockingQueue<Post> QUEUE;
	//The number of pages to fetch on the initial scrape
	private final int INITIAL_PAGE_COUNT = 1;
	private int count = 0;

	/**
	 * Creates a new raw reddit spout for the provided sub-reddit
	 * @param subReddit The sub-reddit to use for the spout
	 */
	public RawRedditSpout(String subReddit){
		SUBREDDIT = subReddit;
		URL = "http://www.reddit.com/r/" + SUBREDDIT + "/new/.json?sort=new&limit=100";
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
		if(QUEUE.size() < 1){
			HttpParams parameters = new BasicHttpParams();
			
			parameters.setParameter(CoreProtocolPNames.PROTOCOL_VERSION, HttpVersion.HTTP_1_1);
			parameters.setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET, "ISO-8859-1");
			parameters.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true);
			parameters.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8192);
			parameters.setParameter(CoreProtocolPNames.USER_AGENT, "DistributedRedditAnalyser /u/Technicolour/");
			
			DefaultHttpClient httpClient = new DefaultHttpClient(parameters);
	
			try {
				
				if(initialPull){
					String lastItemId = "";
					for(int i = 0; i < INITIAL_PAGE_COUNT; i++){
						HttpGet getRequest = new HttpGet(URL +"&count=" + count + "&after=" + lastItemId);
						ResponseHandler<String> responseHandler = new BasicResponseHandler();
						
						String responseBody = httpClient.execute(getRequest, responseHandler);
						
						JSONParser parser= new JSONParser();
						
						JSONObject wrappingObject = (JSONObject) parser.parse(responseBody);
						
						JSONObject wrappingObjectData = (JSONObject) wrappingObject.get("data");
						
						JSONArray children = (JSONArray) wrappingObjectData.get("children");
						
						//Debugging line for testing limits
						//System.out.printf("There are %d children in %s\r\n", children.size(), getRequest.getURI().toString());
						
						if(children.size() == 0)
							break;
						
						//reverse order so printed order is consistent
						for(int c=children.size()-1; c>=0; c--){
							JSONObject childData = (JSONObject) ((JSONObject) children.get(c)).get("data");
							QUEUE.add(new Post((String) childData.get("title"), SUBREDDIT));
						}
						
						lastItemId = (String) wrappingObjectData.get("after");
						
						if(i == 0){
							latestTimestamp = ((Double) ((JSONObject)((JSONObject) children.get(0)).get("data")).get("created")).longValue();
						}
						
						//Rate limit
						if(i != INITIAL_PAGE_COUNT - 1)
							Utils.sleep(1000);
						count += 100;
					}
					initialPull = false;
				}else{
					//Rate limit for the API (pages are cached for 30 seconds)
					Utils.sleep(30000);
					HttpGet getRequest = new HttpGet(URL);
					ResponseHandler<String> responseHandler = new BasicResponseHandler();
		            
					String responseBody = httpClient.execute(getRequest, responseHandler);
					
					JSONParser parser= new JSONParser();
					
					JSONObject wrappingObject = (JSONObject) parser.parse(responseBody);
					
					JSONObject wrappingObjectData = (JSONObject) wrappingObject.get("data");
					
					JSONArray children = (JSONArray) wrappingObjectData.get("children");
					
					if(children.size() > 0){
						//reverse order so it is an actual stream
						for(int c=children.size()-1; c>=0; c--){
							JSONObject childData = (JSONObject) ((JSONObject) children.get(c)).get("data");
							if(latestTimestamp < ((Double) childData.get("created")).longValue())
								QUEUE.add(new Post((String) childData.get("title"), SUBREDDIT));
						}
						
						
						latestTimestamp = ((Double) ((JSONObject)((JSONObject) children.get(0)).get("data")).get("created")).longValue();
					}
				}
			} catch (ClientProtocolException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
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

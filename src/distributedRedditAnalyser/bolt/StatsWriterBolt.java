package distributedRedditAnalyser.bolt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * bolt that writes statistic to a csv file so they can be graphed, analyzed etc.
 * @author tony
 *
 */
public class StatsWriterBolt extends BaseRichBolt {
	private static final long serialVersionUID = -2245664448752984871L;
	private String name = "";
	private String folderName;
	private FileWriter writer;
	
	public StatsWriterBolt(String name, String folderName){
		this.name = name;
		this.folderName = folderName;
	}
	
	public StatsWriterBolt(){}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		if(folderName != null){
			File f = new File("results\\" + folderName);
			f.mkdirs();
		}
		try {
			writer = new FileWriter(folderName == null ? String.format("%s.csv", name) : String.format("results\\%s\\%s.csv", folderName, name));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

	@Override
	public void execute(Tuple input) {
		Long n = input.getLong(0);
		Double a = input.getDouble(1);
		Double k = input.getDouble(2);
		try {
			writer.write(n + "," + a + "," + k + "\n");
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
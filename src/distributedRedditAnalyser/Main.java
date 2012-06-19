package distributedRedditAnalyser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import weka.core.Attribute;
import weka.core.Instances;

import distributedRedditAnalyser.bolt.DistributedOzaBoostBolt;
import distributedRedditAnalyser.bolt.InstanceBolt;
import distributedRedditAnalyser.bolt.OzaBoostBolt;
import distributedRedditAnalyser.bolt.PrinterBolt;
import distributedRedditAnalyser.bolt.StatisticsBolt;
import distributedRedditAnalyser.bolt.StatsPrinterBolt;
import distributedRedditAnalyser.bolt.StatsWriterBolt;
import distributedRedditAnalyser.bolt.StringToWordVectorBolt;
import distributedRedditAnalyser.spout.RawRedditSpout;
import distributedRedditAnalyser.spout.SimRedditSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Using multiple sub-reddits tries to predict what sub-reddit a post came from based on it's topic.
 * 
 * Uses storm to distribute the task over a potential cluster
 * 
 * Built against storm 0.7.2 (https://github.com/nathanmarz/storm/tree/0.7.2) (https://github.com/downloads/nathanmarz/storm/storm-0.7.2-rc1.zip)
 * 
 * @author Luke Barnett 1109967
 * @author Tony Chen 1111377
 *
 */
public class Main {

	/**
	 * Main entry point for the program.
	 * 
	 * Takes a list of sub-reddits to perform the analysis with
	 * 
	 * @param args The sub-reddits (case sensitive) to use 
	 */
	public static void main(String[] args) {
		
		if(args.length == 0){
			System.err.println("No sub-reddits given, unable to continue");
			showUsage();
			System.exit(2);
		}
		if(args.length < 2){
			System.err.println("Need to provide at least two subreddits");
			showUsage();
			System.exit(2);
		}
		
		//Copy the sub-reddits into a separate list for future use with more complex arguments
		ArrayList<String> subreddits = new ArrayList<String>();
		for(int i = 0; i < args.length; i++){
			subreddits.add(args[i]);
		}
		
		//TUNE PARAMETERS HERE
		final int STAT_RES = 5;
		final int FILTER_SET_SIZE = args.length * 100;//batch size is 100 posts per subreddit
		final int WORDS_TO_KEEP = 200; //need larger word vectors for better results
		final int RUNTIME = 300 * (60000);//change first term to number of minutes
		
		//Build the Instances Header
		ArrayList<Attribute> att = new ArrayList<Attribute>();
		att.add(new Attribute("title", (List<String>)null, 0));
		att.add(new Attribute("redditClass", subreddits, 1));
		Instances instHeaders = new Instances("reddit",att, 10);
		instHeaders.setClassIndex(1);
		
		/**
		 * Start building the topology
		 * Examples can be found at: https://github.com/nathanmarz/storm-starter
		 */
		
		//Create the topology builder
		TopologyBuilder builder = new TopologyBuilder();
		
		BoltDeclarer instanceBolt = builder.setBolt("instancebolt", new InstanceBolt(instHeaders));
		
		String resultsFolder = ((Long)System.currentTimeMillis()).toString();
		
		//Add a spout for each sub-reddit
		for(String subreddit : subreddits){
			resultsFolder = String.format("[%s]", subreddit) + resultsFolder;
			builder.setSpout("raw:" + subreddit, new RawRedditSpout(subreddit));
			instanceBolt.shuffleGrouping("raw:" + subreddit);
		}
		
		builder.setBolt("stringToWordBolt", new StringToWordVectorBolt(FILTER_SET_SIZE, WORDS_TO_KEEP, instHeaders)).shuffleGrouping("instancebolt");
		
		//NaiveBayesMultinomial
		/*builder.setBolt("ozaBoostBolt:naiveBayesMultinomial", new OzaBoostBolt("bayes.NaiveBayesMultinomial")).shuffleGrouping("stringToWordBolt");
		builder.setBolt("statistics:naiveBayesMultinomial", new StatisticsBolt(subreddits.size(),STAT_RES)).shuffleGrouping("ozaBoostBolt:naiveBayesMultinomial");
		
		builder.setBolt("StatsPrinterBolt:naiveBayesMultinomial", new StatsPrinterBolt("naiveBayesMultinominal")).shuffleGrouping("statistics:naiveBayesMultinomial");
		builder.setBolt("StatsWriterBolt:naiveBayesMultinomial", new StatsWriterBolt("naiveBayesMultinominal", resultsFolder)).shuffleGrouping("statistics:naiveBayesMultinomial");
		
		//NaiveBayes
		builder.setBolt("ozaBoostBolt:naiveBayes", new OzaBoostBolt("bayes.NaiveBayes")).shuffleGrouping("stringToWordBolt");
		builder.setBolt("statistics:naiveBayes", new StatisticsBolt(subreddits.size(),STAT_RES)).shuffleGrouping("ozaBoostBolt:naiveBayes");
		
		builder.setBolt("StatsPrinterBolt:naiveBayes", new StatsPrinterBolt("naiveBayes")).shuffleGrouping("statistics:naiveBayes");
		builder.setBolt("StatsWriterBolt:naiveBayes", new StatsWriterBolt("naiveBayes", resultsFolder)).shuffleGrouping("statistics:naiveBayes");
		
		//Perceptron
		builder.setBolt("ozaBoostBolt:perceptron", new OzaBoostBolt("functions.Perceptron")).shuffleGrouping("stringToWordBolt");
		builder.setBolt("statistics:perceptron", new StatisticsBolt(subreddits.size(),STAT_RES)).shuffleGrouping("ozaBoostBolt:perceptron");
		
		builder.setBolt("StatsPrinterBolt:perceptron", new StatsPrinterBolt("perceptron")).shuffleGrouping("statistics:perceptron");
		builder.setBolt("StatsWriterBolt:perceptron", new StatsWriterBolt("perceptron", resultsFolder)).shuffleGrouping("statistics:perceptron");*/
		
		//Distributed OzaBoost
		//NaiveBayesMultinomial
		//Create the bolts
		BoltDeclarer naiveBayesMultinomial1 = builder.setBolt("ozaBoostBolt:naiveBayesMultinomial1", new DistributedOzaBoostBolt("bayes.NaiveBayesMultinomial", 1)).shuffleGrouping("stringToWordBolt");
		BoltDeclarer naiveBayesMultinomial2 = builder.setBolt("ozaBoostBolt:naiveBayesMultinomial2", new DistributedOzaBoostBolt("bayes.NaiveBayesMultinomial", 2)).shuffleGrouping("stringToWordBolt");
		BoltDeclarer naiveBayesMultinomial3 = builder.setBolt("ozaBoostBolt:naiveBayesMultinomial3", new DistributedOzaBoostBolt("bayes.NaiveBayesMultinomial", 3)).shuffleGrouping("stringToWordBolt");
		
		//Add each other to each other
		naiveBayesMultinomial1.shuffleGrouping("ozaBoostBolt:naiveBayesMultinomial2");
		naiveBayesMultinomial1.shuffleGrouping("ozaBoostBolt:naiveBayesMultinomial3");
		
		naiveBayesMultinomial2.shuffleGrouping("ozaBoostBolt:naiveBayesMultinomial1");
		naiveBayesMultinomial2.shuffleGrouping("ozaBoostBolt:naiveBayesMultinomial3");
		
		naiveBayesMultinomial3.shuffleGrouping("ozaBoostBolt:naiveBayesMultinomial2");
		naiveBayesMultinomial3.shuffleGrouping("ozaBoostBolt:naiveBayesMultinomial1");
		
		//Only subscribed to the first bolt for statistics
		builder.setBolt("statistics:naiveBayesMultinomial", new StatisticsBolt(subreddits.size(),STAT_RES)).shuffleGrouping("ozaBoostBolt:naiveBayesMultinomial1");
		
		builder.setBolt("statsPrinterBolt:naiveBayesMultinomial", new StatsPrinterBolt("naiveBayesMultinominal")).shuffleGrouping("statistics:naiveBayesMultinomial");
		builder.setBolt("statsWriterBolt:naiveBayesMultinomial", new StatsWriterBolt("naiveBayesMultinominal", resultsFolder)).shuffleGrouping("statistics:naiveBayesMultinomial");
		
		//NaiveBayes
		//Create the bolts
		BoltDeclarer naiveBayes1 = builder.setBolt("ozaBoostBolt:naiveBayes1", new DistributedOzaBoostBolt("bayes.NaiveBayes", 1)).shuffleGrouping("stringToWordBolt");
		BoltDeclarer naiveBayes2 = builder.setBolt("ozaBoostBolt:naiveBayes2", new DistributedOzaBoostBolt("bayes.NaiveBayes", 2)).shuffleGrouping("stringToWordBolt");
		BoltDeclarer naiveBayes3 = builder.setBolt("ozaBoostBolt:naiveBayes3", new DistributedOzaBoostBolt("bayes.NaiveBayes", 3)).shuffleGrouping("stringToWordBolt");
		
		//Add each other to each other
		naiveBayes1.shuffleGrouping("ozaBoostBolt:naiveBayes2");
		naiveBayes1.shuffleGrouping("ozaBoostBolt:naiveBayes3");
		
		naiveBayes2.shuffleGrouping("ozaBoostBolt:naiveBayes1");
		naiveBayes2.shuffleGrouping("ozaBoostBolt:naiveBayes3");
		
		naiveBayes3.shuffleGrouping("ozaBoostBolt:naiveBayes2");
		naiveBayes3.shuffleGrouping("ozaBoostBolt:naiveBayes1");
		
		//Only subscribed to the first bolt for statistics
		builder.setBolt("statistics:naiveBayes", new StatisticsBolt(subreddits.size(),STAT_RES)).shuffleGrouping("ozaBoostBolt:naiveBayes1");
		
		builder.setBolt("statsPrinterBolt:naiveBayes", new StatsPrinterBolt("naiveBayes")).shuffleGrouping("statistics:naiveBayes");
		builder.setBolt("statsWriterBolt:naiveBayes", new StatsWriterBolt("naiveBayes", resultsFolder)).shuffleGrouping("statistics:naiveBayes");
		
		//NaiveBayes
		//Create the bolts
		BoltDeclarer perceptron1 = builder.setBolt("ozaBoostBolt:perceptron1", new DistributedOzaBoostBolt("functions.Perceptron", 1)).shuffleGrouping("stringToWordBolt");
		BoltDeclarer perceptron2 = builder.setBolt("ozaBoostBolt:perceptron2", new DistributedOzaBoostBolt("functions.Perceptron", 2)).shuffleGrouping("stringToWordBolt");
		BoltDeclarer perceptron3 = builder.setBolt("ozaBoostBolt:perceptron3", new DistributedOzaBoostBolt("functions.Perceptron", 3)).shuffleGrouping("stringToWordBolt");
		
		//Add each other to each other
		perceptron1.shuffleGrouping("ozaBoostBolt:perceptron2");
		perceptron1.shuffleGrouping("ozaBoostBolt:perceptron3");
		
		perceptron2.shuffleGrouping("ozaBoostBolt:perceptron1");
		perceptron2.shuffleGrouping("ozaBoostBolt:perceptron3");
		
		perceptron3.shuffleGrouping("ozaBoostBolt:perceptron2");
		perceptron3.shuffleGrouping("ozaBoostBolt:perceptron1");
		
		//Only subscribed to the first bolt for statistics
		builder.setBolt("statistics:perceptron", new StatisticsBolt(subreddits.size(),STAT_RES)).shuffleGrouping("ozaBoostBolt:perceptron1");
		
		builder.setBolt("statsPrinterBolt:perceptron", new StatsPrinterBolt("perceptron")).shuffleGrouping("statistics:perceptron");
		builder.setBolt("statsWriterBolt:perceptron", new StatsWriterBolt("perceptron", resultsFolder)).shuffleGrouping("statistics:perceptron");
		
		
		//Create the configuration object
		Config conf = new Config();
		
		//Create a local cluster
		LocalCluster cluster = new LocalCluster();
		
		//Submit the topology to the cluster for execution
		cluster.submitTopology("redditAnalyser", conf, builder.createTopology());
		
		//Give a timeout period
		Utils.sleep(RUNTIME);
		
		//Kill the topology first
		cluster.killTopology("redditAnalyser");
		
		//Close the cluster
		cluster.shutdown();
		
	}
	
	/**
	 * Prints the required usage of the program
	 */
	private static void showUsage(){
		System.out.println("Usage:");
		System.out.println("java distributedRedditAnalyser.Main subreddit subreddit [subreddit...]");
	}

}

package com.datasalt.trident.solved;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.datasalt.trident.FakeTweetsBatchSpout;
import com.datasalt.trident.Utils;

/**
 * This is a simple example on how to partition a stream by some fields. We partition by actor and save the last 5
 * tweets of the actor in a function.
 */
public class LastTweetsTopology {

	@SuppressWarnings("serial")
	public static class LastTweets extends BaseFunction {

		private Map<String, List<String>> lastTweets = new HashMap<String, List<String>>();
		private int n;

		public LastTweets(int n) {
			this.n = n;
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String actor = (String) tuple.get(1);
			String tweet = (String) tuple.get(0);

			List<String> prevlist = lastTweets.get(actor);
			if(prevlist == null) {
				prevlist = new LinkedList<String>();
				lastTweets.put(actor, prevlist);
			}
			prevlist.add(tweet.substring(0, Math.min(10, tweet.length())) + "...");
			if(prevlist.size() > n) {
				prevlist.remove(0);
			}
			collector.emit(new Values(prevlist));
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout).partitionBy(new Fields("actor"))
		    .each(new Fields("text", "actor"), new LastTweets(3), new Fields("last_tweets"))
		    .parallelismHint(5).each(new Fields("actor", "last_tweets"), new Utils.PrintFilter());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", new Config(), buildTopology(drpc));
	}
}

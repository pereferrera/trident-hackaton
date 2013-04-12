package com.datasalt.trident.examples;

import java.io.IOException;
import java.util.Map;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.datasalt.trident.FakeTweetsBatchSpout;
import com.datasalt.trident.Utils;

/**
 * This example is useful for understanding how parallelism and partitioning works. parallelismHit() is applied down
 * until the next partitioning operation. Therefore here we have 5 processes (Bolts) applying a filter and 2 processes
 * creating messages (Spouts).
 * <p>
 * But because we are partitioning by actor and applying a filter that only keeps tweets from one actor, we see in
 * stderr that it is always the same partition who is filtering the tweets, which makes sense.
 * <p>
 * Now comment out the partitionBy() and uncomment the shuffle(), what happens?
 * 
 * @author pere
 */
public class ParallelismExample1 {

	@SuppressWarnings("serial")
	public static class PerActorTweetsFilter extends BaseFilter {

		private int partitionIndex;
		private String actor;

		public PerActorTweetsFilter(String actor) {
			this.actor = actor;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			this.partitionIndex = context.getPartitionIndex();
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			boolean filter = tuple.getString(0).equals(actor);
			if(filter) {
				System.err.println("I am partition [" + partitionIndex + "] and I have kept a tweet by: "
				    + actor);
			}
			return filter;
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout)
				.parallelismHint(2)
				.partitionBy(new Fields("actor"))
		    // .shuffle()
		    .each(new Fields("actor", "text"), new PerActorTweetsFilter("dave")).parallelismHint(5)
		    .each(new Fields("actor", "text"), new Utils.PrintFilter());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();

		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", conf, buildTopology(drpc));
	}
}

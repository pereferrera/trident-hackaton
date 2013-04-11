package com.datasalt.trident.examples;

import java.io.IOException;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.datasalt.trident.FakeTweetsBatchSpout;
import com.datasalt.trident.Utils;

/**
 * This example illustrates the usage of groupBy. GroupBy creates a "grouped stream" which means that subsequent aggregators
 * will only affect Tuples within a group. GroupBy must always be followed by an aggregator. Because we are aggregating groups,
 * we don't need to produce a hashmap for the per-location counts (as opposed to {@link PerLocationCounts1} and we can use the 
 * simple Count() aggregator.
 * <p>
 * To better understand the different types of aggregations you can read: https://github.com/nathanmarz/storm/wiki/Trident-API-Overview .
 *  
 * @author pere
 */
public class PerLocationCounts2 {

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(100);

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout)
			.groupBy(new Fields("location"))
			.aggregate(new Fields("location"), new Count(), new Fields("count"))
			.each(new Fields("location", "count"), new Utils.PrintFilter());
		
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();

		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", conf, buildTopology(drpc));
	}
}

package com.datasalt.trident;

import java.io.IOException;

import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * Use this skeleton for starting your own topology that uses the Fake tweets generator as data source.
 * 
 * @author pere
 */
public class Skeleton {

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout).each(new Fields("id", "text", "actor", "location", "date"),
		    new Utils.PrintFilter());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();

		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", conf, buildTopology(drpc));
	}
}

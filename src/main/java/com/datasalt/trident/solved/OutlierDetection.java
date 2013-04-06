package com.datasalt.trident.solved;

import java.io.IOException;
import java.util.Map;

import storm.trident.TridentTopology;
import storm.trident.operation.Filter;
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
 * A simple example of filtering. Only outliers pass the filter.
 */
public class OutlierDetection {

	@SuppressWarnings("serial")
	public static class OutlierFilter implements Filter {

		int position;

		public OutlierFilter(int position) {
			this.position = position;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
		}

		@Override
		public void cleanup() {
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			return tuple.getString(position).contains("bomb");
			// return tuple.getString(position).length() < 10;
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();
		TridentTopology topology = new TridentTopology();

		topology.newStream("hackaton", spout)
		    .each(new Fields("actor", "text", "date"), new OutlierFilter(1))
		    .each(new Fields("actor", "text", "date"), new Utils.PrintFilter()).parallelismHint(5);

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", new Config(), buildTopology(drpc));
	}
}

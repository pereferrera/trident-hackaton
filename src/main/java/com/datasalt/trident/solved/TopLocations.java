package com.datasalt.trident.solved;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
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
 * A running real-time count by location. Persisted using Trident state API. Counts are partitioned and each partition
 * emits the Top N. The aggregated tops are then persisted uniquely.
 */
public class TopLocations {

	@SuppressWarnings("serial")
	public static class TopNFunction extends BaseFunction {

		private Map<String, Integer> totalCounts = new HashMap<String, Integer>();
		private int n;

		public TopNFunction(int n) {
			this.n = n;
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String key = (String) tuple.get(0);
			// update counts
			totalCounts.put(key, MapUtils.getInteger(totalCounts, key, 0) + 1);
			// emit top n
			System.err.println(Thread.currentThread().getName() + ", I have: " + totalCounts);
			collector.emit(new Values(Utils.getTopNOfMap(totalCounts, n)));
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(50);

		StateFactory mapState = new MemoryMapState.Factory();

		TridentTopology topology = new TridentTopology();
		topology
		    .newStream("spout", spout)
		    .partitionBy(new Fields("location"))
		    .each(new Fields("location"), new TopNFunction(2), new Fields("tops"))
		    .parallelismHint(3)
		    .global()
		    .persistentAggregate(mapState, new Fields("tops"), new Utils.CountAggregator(),
		        new Fields("aggregated_tops")).newValuesStream()
		    .each(new Fields("aggregated_tops"), new Utils.PrintFilter())
		    .parallelismHint(1);

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", new Config(), buildTopology(drpc));
	}
}

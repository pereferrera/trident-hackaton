package com.datasalt.trident.solved;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.MapUtils;

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
 * An example of an scalable top of tops.
 * We partition by location and calculate the counts for each location. We emit the top locations so far.
 * Then we perform a global grouping to a single process and perform the top of tops.
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
			collector.emit(new Values(Utils.getTopNOfMap(totalCounts, n)));
		}
	}

	@SuppressWarnings("serial")
	public static class TopOfTops extends BaseFunction {

		private Map<String, Integer> totalCounts = new HashMap<String, Integer>();
		private int n;

		public TopOfTops(int n) {
			this.n = n;
		}

		private Thread t = null;

		@Override
		@SuppressWarnings("unchecked")
		public void execute(TridentTuple tuple, TridentCollector collector) {
			// set counts that come from previous phase
			List<Map.Entry<String, Integer>> partialTop = (List<Entry<String, Integer>>) tuple.get(0);
			for(Map.Entry<String, Integer> entry : partialTop) {
				totalCounts.put(entry.getKey(), entry.getValue());
			}

			// this is how we need to do politeness for making things efficient:
			if(t == null) {
				final TridentCollector refCopy = collector;
				t = new Thread() {
					public void run() {
						try {
							while(true) {
								// emit top n
								refCopy.emit(new Values(Utils.getTopNOfMap(totalCounts, n)));
								Thread.sleep(1000);
							}
						} catch(InterruptedException e) {
							// bye bye!
						}
					};
				};
				t.start();
			}
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(50);

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout)
				.partitionBy(new Fields("location"))
		    .each(new Fields("location"), new TopNFunction(3), new Fields("tops")).parallelismHint(5)
		    .global()
		    .each(new Fields("tops"), new TopOfTops(3), new Fields("top_of_tops"))
		    .each(new Fields("top_of_tops"), new Utils.PrintFilter());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", new Config(), buildTopology(drpc));
	}
}

package com.datasalt.trident.solved;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.Split;
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
 * A complex example that combines 3 functions, partitioning and complex custom logic
 * for calculating the trending hashtags in a window of time.
 */
@SuppressWarnings({ "serial" })
public class TrendingHashTagsTopology {

	public static class HashTagFilter extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String word = tuple.getString(0);
			if(word.startsWith("#")) {
				// only emit hashtags, and emit them without the # character
				collector.emit(new Values(word.substring(1, word.length())));
			}
		}
	}

	public static class ParseDate extends BaseFunction {

		private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss aa");
		
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			try {
				collector.emit(new Values(DATE_FORMAT.parse((String) tuple.get(1))
				    .getTime()));
			} catch(ParseException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static class RotatingTimeWindow extends BaseFunction {

		private ConcurrentHashMap<String, CopyOnWriteArrayList<Long>> map = new ConcurrentHashMap<String, CopyOnWriteArrayList<Long>>();
		private volatile TridentCollector collector = null;
		private long timeWindow;
	  // will be instantiated when first tuple is received
		// otherwise the thread is serialized and collector will always be null
		private Thread reporter = null; 

		public RotatingTimeWindow(long timeWindow) { // in milliseconds
			this.timeWindow = timeWindow;
		}
		
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			if(this.collector == null) {
				this.collector = collector;
				reporter = new Thread() {
					public void run() {
						try {
							while(true) {
								for(Map.Entry<String, CopyOnWriteArrayList<Long>> mapEntry : map.entrySet()) {
									RotatingTimeWindow.this.collector.emit(new Values(mapEntry.getKey(), mapEntry.getValue().size()));
									// expire old timestamps
									long timeLimit = System.currentTimeMillis() - timeWindow;
									for(int i = 0; i < mapEntry.getValue().size(); i++) {
										if(mapEntry.getValue().get(i) < timeLimit) {
											// expire
											mapEntry.getValue().remove(i);
										}
									}
								}
								Thread.sleep(1000);
							}
						} catch(InterruptedException e) {
							// bye bye!
						}
					};
				};
				reporter.start();
			}
			String key = (String) tuple.get(0);
			CopyOnWriteArrayList<Long> prevMap = map.get(key);
			if(prevMap == null) {
				prevMap = new CopyOnWriteArrayList<Long>();
				map.put(key, prevMap);
			}
			prevMap.add((Long) tuple.get(1));
		}

		@Override
		public void cleanup() {
			if(reporter != null) {
				reporter.interrupt();
			}
			super.cleanup();
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

		TridentTopology topology = new TridentTopology();

		topology.newStream("spout", spout)
				.each(new Fields("text", "date"), new Split(), new Fields("word"))
		    .each(new Fields("word", "date"), new HashTagFilter(), new Fields("hashtag"))
		    .each(new Fields("hashtag", "date"), new ParseDate(), new Fields("timestamp"))
		    .partitionBy(new Fields("hashtag"))
		    .each(new Fields("hashtag", "timestamp"), new RotatingTimeWindow(5000), new Fields("key", "count"))
		    .each(new Fields("key", "count"), new Utils.PrintFilter());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", new Config(), buildTopology(drpc));
	}
}

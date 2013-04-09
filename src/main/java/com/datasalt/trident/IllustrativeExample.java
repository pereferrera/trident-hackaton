package com.datasalt.trident;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;

import storm.trident.TridentTopology;
import storm.trident.operation.Aggregator;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class IllustrativeExample {

	@SuppressWarnings("serial")
  public static class PereTweetsFilter implements Filter {

		int partitionIndex;
		
		@SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
			this.partitionIndex = context.getPartitionIndex();
    }
		@Override
    public void cleanup() {
    }
		@Override
    public boolean isKeep(TridentTuple tuple) {
	    boolean filter = tuple.getString(1).equals("pere");
	    if(filter) {
	    	System.err.println("I am partition [" + partitionIndex + "] and I have filtered pere.");	
	    } else {
	    	
	    }
	    return filter;
    }
	}
	
	@SuppressWarnings("serial")
  public static class UppercaseFunction extends BaseFunction {

		@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
	    collector.emit(new Values(tuple.getString(0).toUpperCase()));
    }
	}
	
	@SuppressWarnings("serial")
  public static class LocationAggregator implements Aggregator<Map<String, Integer>> {

		int partitionId;
		
		@SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
			this.partitionId = context.getPartitionIndex();
    }
		@Override
    public void cleanup() {
    }
		@Override
    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
	    return new HashMap<String, Integer>();
    }
		@Override
    public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
			String loc = tuple.getString(0);
			val.put(loc, MapUtils.getInteger(val, loc, 0) + 1);
    }
		@Override
    public void complete(Map<String, Integer> val, TridentCollector collector) {
	    // update whatever database with "val" 
			System.err.println("I am partition [" + partitionId + "] and have aggregated: [" + val + "]");
			collector.emit(new Values(val));
    }
	}
	
	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

		// A topology is a set of streams.
		// A stream is a DAG of Spouts and Bolts.
		// (In Storm there are Spouts (data producers) and Bolts (data processors).
		// Spouts create Tuples and Bolts manipulate then and possibly emit new ones.)

		// But in Trident we operate at a higher level.
		// Bolts are created and connected automatically out of higher-level constructs.
		// Also, Spouts are "batched".
		TridentTopology topology = new TridentTopology();
		
		// Each primitive allows us to apply either filters or functions to the stream
		// We always have to select the input fields.
		topology.newStream("filter", spout)
			.each(new Fields("text", "actor"), new PereTweetsFilter())
		  .each(new Fields("text", "actor"), new Utils.PrintFilter());

		// Functions describe their output fields, which are always appended to the input fields.
		topology.newStream("function", spout)
		  .each(new Fields("text", "actor"), new UppercaseFunction(), new Fields("uppercased_text"))
		  .each(new Fields("text", "uppercased_text"), new Utils.PrintFilter());

		// As you see, Each operations can be chained.
		
		// Stream can be parallelized with "parallelismHint"
		// Parallelism hint is applied downwards until a partitioning operation (we will see this later).
		// This topology creates 5 spouts and 5 bolts:
		// Let's debug that with TridentOperationContext . partitionIndex !
		topology.newStream("parallel", spout)
			.each(new Fields("text", "actor"), new PereTweetsFilter())
			.parallelismHint(5)
			.each(new Fields("text", "actor"), new Utils.PrintFilter());
		
		// A stream can be partitioned in various ways.
		// Let's partition it by "actor". What happens with previous example?
		topology.newStream("parallel_and_partitioned", spout)
			.partitionBy(new Fields("actor"))
			.each(new Fields("text", "actor"), new PereTweetsFilter())
			.parallelismHint(5)
			.each(new Fields("text", "actor"), new Utils.PrintFilter());
		
		// Only one partition is filtering, which makes sense for the case.
		// If we remove the partitionBy we get the previous behavior.
		
		// Before we have parallelism = 5 everywhere. What if we want only one spout?
		// We need to specify a partitioning policy for that to happen.
		// (We said that parallelism hint is applied downwards until a partitioning operation is found).
		
		// But if we don't want to partition by any field, we can just use shuffle()
		// We could also choose global() - with care!
		topology.newStream("parallel_and_partitioned", spout)
			.parallelismHint(1)
			.shuffle()
			.each(new Fields("text", "actor"), new PereTweetsFilter())
			.parallelismHint(5)
			.each(new Fields("text", "actor"), new Utils.PrintFilter());
		
		// Because data is batched, we can aggregate batches for efficiency.
		// The aggregate primitive aggregates one full batch. Useful if we want to persist the result of each batch only once.
		// The aggregation for each batch is executed in a random partition as can be seen:
		topology.newStream("aggregation", spout)
			.parallelismHint(1)
			.aggregate(new Fields("location"), new LocationAggregator(), new Fields("aggregated_result"))
			.parallelismHint(5)
			.each(new Fields("aggregated_result"), new Utils.PrintFilter());

		// The partitionAggregate on the other hand only executes the aggregator within one partition's part of the batch.
		// Let's debug that with TridentOperationContext . partitionIndex !
		topology.newStream("partial_aggregation", spout)
			.parallelismHint(1)
			.shuffle()
			.partitionAggregate(new Fields("location"), new LocationAggregator(), new Fields("aggregated_result"))
			.parallelismHint(6)
			.each(new Fields("aggregated_result"), new Utils.PrintFilter());

		// (See what happens when we change the Spout batch size / parallelism)

		// A useful primitive is groupBy.
		// It splits the stream into groups so that aggregations only ocurr within a group.
		// Because now we are grouping, the aggregation function can be much simpler (Count())
		// We don't need to use HashMaps anymore.
		topology.newStream("aggregation", spout)
			.parallelismHint(1)
			.groupBy(new Fields("location"))
			.aggregate(new Fields("location"), new Count(), new Fields("count"))
			.parallelismHint(5)
			.each(new Fields("location", "count"), new Utils.PrintFilter());
		
		// EXERCISE: Use Functions and Aggregators to parallelize per-hashtag counts.
		// Step by step: 1) Obtain and select hashtags, 2) Write the Aggregator.
		
		// Bonus 1: State API.
		// Bonus 2: "Trending" hashtags. 
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();

		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", conf, buildTopology(drpc));
	}
}

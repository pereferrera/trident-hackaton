package com.datasalt.trident;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class Utils {

	/**
	 * An example BaseFunction. It lowercases the String value in the specified Tuple position.
	 */
	@SuppressWarnings("serial")
	public static class LowercaseFunction extends BaseFunction {

		private int fieldIndex;

		public LowercaseFunction(int fieldIndex) {
			this.fieldIndex = fieldIndex;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			System.err.println("Prepare LowercaseFunction");
			super.prepare(conf, context);
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			collector.emit(new Values(((String) tuple.get(fieldIndex)).toLowerCase()));
		}

	}

	/**
	 * A filter that filters nothing but prints the tuples it sees. Useful to test things.
	 */
	@SuppressWarnings("serial")
	public static class PrintFilter implements Filter {

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
		}

		@Override
		public void cleanup() {
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			System.out.println(tuple);
			return true;
		}
	}

	/**
	 * Given a hashmap with string keys and integer counts, returns the "top" map of it. "n" specifies the size of
	 * the top to return.
	 */
	public final static Map<String, Integer> getTopNOfMap(Map<String, Integer> map, int n) {
		List<Map.Entry<String, Integer>> entryList = new ArrayList<Map.Entry<String, Integer>>(map.size());
		entryList.addAll(map.entrySet());
		Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
				return arg1.getValue().compareTo(arg0.getValue());
			}
		});
		Map<String, Integer> toReturn = new HashMap<String, Integer>();
		for(Map.Entry<String, Integer> entry: entryList.subList(0, Math.min(entryList.size(), n))) {
			toReturn.put(entry.getKey(), entry.getValue());
		}
		return toReturn;
	}
	
	/**
	 * A Trident aggregator suitable for persisting a set of counts (String -> Integer).
	 * For simplicity, it overrides old values with new values.
	 */
	@SuppressWarnings("serial")
  public static class CountAggregator implements CombinerAggregator<Map<String, Integer>> {

		@SuppressWarnings("unchecked")
    @Override
    public Map<String, Integer> init(TridentTuple tuple) {
			Map<String, Integer> vals = zero();
			vals.putAll((Map<String, Integer>) tuple.get(0));
			return vals;
		}

		@Override
    public Map<String, Integer> combine(Map<String, Integer> oldVal, Map<String, Integer> newVal) {
			// easy: new values always override old ones
			oldVal.putAll(newVal);
	    return oldVal;
    }

		@Override
    public Map<String, Integer> zero() {
	    return new HashMap<String, Integer>(); 
    }
	}
}

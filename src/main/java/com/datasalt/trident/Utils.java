package com.datasalt.trident;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.MapUtils;

import storm.trident.operation.Aggregator;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class Utils {

	/**
	 * An example Aggregator. Useful for saving (key, value) counts in a database.
	 */
	@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
  public static class KeyCountAggregator implements Aggregator {

		@Override
    public void prepare(Map conf, TridentOperationContext context) {
		}
		@Override
    public void cleanup() {
    }
		@Override
    public Object init(Object batchId, TridentCollector collector) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("batchId", batchId);
	    return map;
    }
		@Override
    public void aggregate(Object val, TridentTuple tuple, TridentCollector collector) {
			String key = (String) tuple.get(0);
			Map<String, Object> map = (Map<String, Object>)val; 
			map.put(key, MapUtils.getInteger(map, key, 0) + 1);
    }
		@Override
    public void complete(Object val, TridentCollector collector) {
			collector.emit(new Values(val));
    }
	}
	
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
			collector.emit(new Values(((String)tuple.get(fieldIndex)).toLowerCase()));
    }
		
	}

	/**
	 * A filter that filters nothing but prints the tuples it sees.
	 * Useful to test things.
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
	 * Given a hashmap with string keys and integer counts, returns the "top" entry set of it.
	 * "n" specifies the size of the top to return.
	 */
	public final static List<Map.Entry<String, Integer>> getTopNOfMap(Map<String, Integer> map, int n) {
		List<Map.Entry<String, Integer>> entryList = new ArrayList<Map.Entry<String, Integer>>(map.size());
		entryList.addAll(map.entrySet());
		Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {

			@Override
      public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
        return arg1.getValue().compareTo(arg0.getValue());
      }
		});
		return entryList.subList(0, Math.min(entryList.size(), n));
	}
}

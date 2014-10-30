package com.amadeus.pcb.parse;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.Iterator;

public class OneWeekFilter {
	
	@SuppressWarnings("serial")
	public static class WeekFilter implements FilterFunction<String> {

		private static ArrayList<String> dateWhiteList = new ArrayList<String>() {{
			add("140502");add("140503");add("140504");add("140505");
			add("140506");add("140507");add("140508");
			}};
			
		@Override
		public boolean filter(String value) throws Exception {
			for (Iterator<String> iterator = dateWhiteList.iterator(); iterator.hasNext();) {
				String string = (String) iterator.next();
				if(value.startsWith(string))
					return true;
			}
			return false;
		}
		
	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		
		DataSet<String> flights = env.readTextFile("file:///home/robert/Amadeus/data/PRD.NGI.AIROS.SSIM7.D140206.txt");
		flights.filter(new WeekFilter()).setParallelism(1).writeAsText("file:///home/robert/Amadeus/data/oneweek.txt");
		env.execute();
	}

}

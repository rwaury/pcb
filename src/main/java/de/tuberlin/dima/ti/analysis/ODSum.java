package de.tuberlin.dima.ti.analysis;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.HashSet;

// aggregate OD estimate over whole period (only to compare to AirTraffic data)
public class ODSum implements GroupReduceFunction<Itinerary, Tuple5<String, String, Integer, Integer, Double>> {

    @Override
    public void reduce(Iterable<Itinerary> iterable, Collector<Tuple5<String, String, Integer, Integer, Double>> out) throws Exception {
        Tuple5<String, String, Integer, Integer, Double> result = new Tuple5<String, String, Integer, Integer, Double>();
        HashSet<String> days = new HashSet<String>(7);
        int itinCount = 0;
        int paxSum = 0;
        double estimateSum = 0.0;
        for(Itinerary t : iterable) {
            if(!days.contains(t.f2)) {
                days.add(t.f2);
                estimateSum += t.f17;
            }
            result.f0 = t.f0;
            result.f1 = t.f1;
            paxSum += t.f15;
            itinCount++;
        }
        result.f2 = itinCount;
        result.f3 = paxSum;
        result.f4 = estimateSum;
        out.collect(result);
    }
}

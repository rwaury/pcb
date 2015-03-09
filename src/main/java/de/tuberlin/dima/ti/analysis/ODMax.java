package de.tuberlin.dima.ti.analysis;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.HashMap;

// get OD capacity from CB result
public class ODMax implements GroupReduceFunction<Itinerary, Tuple5<String, String, String, Integer, Integer>> {

    @Override
    public void reduce(Iterable<Itinerary> itineraries, Collector<Tuple5<String, String, String, Integer, Integer>> out) throws Exception {
        HashMap<String, Integer> flightLoad = new HashMap<String, Integer>();
        flightLoad.put("", -1);
        int maxODCapacity = 0;
        boolean first = true;
        Tuple5<String, String, String, Integer, Integer> result = new Tuple5<String, String, String, Integer, Integer>();
        for(Itinerary itinerary : itineraries) {
            if(first) {
                result.f0 = itinerary.f0;
                result.f1 = itinerary.f1;
                result.f2 = itinerary.f2;
                first = false;
            }
            int increase1 = 0;
            int increase2 = 0;
            int increase3 = 0;
            Integer itinCap = itinerary.f14;
            Integer cap1 = flightLoad.get(itinerary.f3);
            if(cap1 == null) {
                flightLoad.put(itinerary.f3, itinCap);
                increase1 = itinCap;
            } else if(cap1 <= itinCap) {
                increase1 = itinCap-cap1;
                flightLoad.put(itinerary.f3, itinCap);
            }
            Integer cap2 = flightLoad.get(itinerary.f4);
            if(cap2 == null) {
                flightLoad.put(itinerary.f4, itinCap);
            } else if(cap2 <= itinCap && cap2 > -1) {
                increase2 = itinCap-cap2;
                flightLoad.put(itinerary.f4, itinCap);
            }
            Integer cap3 = flightLoad.get(itinerary.f5);
            if(cap3 == null) {
                flightLoad.put(itinerary.f5, itinCap);
            } else if(cap3 <= itinCap && cap3 > -1) {
                increase3 = itinCap-cap3;
                flightLoad.put(itinerary.f5, itinCap);
            }
            maxODCapacity += Math.max(increase1, Math.max(increase2, increase3));
        }
        result.f3 = 0;
        result.f4 = maxODCapacity;
        out.collect(result);
    }
}
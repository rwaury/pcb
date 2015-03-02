package com.amadeus.ti.analysis;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

// aggregate OD travel time (distance metric)
public class ODDistanceAggregator implements GroupReduceFunction<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>, Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>> {

    @Override
    public void reduce(Iterable<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> values, Collector<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>> out) throws Exception {
        Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector> result =
                new Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>();
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        int sum = 0;
        int count = 0;
        boolean first = true;
        SerializableVector vector = new SerializableVector(TrafficAnalysis.OD_FEATURE_COUNT);
        for (Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer> t : values) {
            if (first) {
                result.f0 = t.f0;
                result.f1 = t.f1;
                result.f2 = t.f5;
                result.f3 = t.f2;
                result.f4 = t.f3;
                result.f5 = t.f4;
                vector.getVector().setEntry(0, t.f6);
                first = false;
            }
            int minutes = t.f7;
            sum += minutes;
            count++;
            if (minutes < min) {
                min = minutes;
            }
            if (minutes > max) {
                max = minutes;
            }
        }
        Double avg = (double) sum / (double) count;
        vector.getVector().setEntry(1, (double) min);
        vector.getVector().setEntry(2, (double) max);
        vector.getVector().setEntry(3, avg);
        vector.getVector().setEntry(4, (double) count);
        result.f6 = vector;
        out.collect(result);
    }
}

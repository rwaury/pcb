package de.tuberlin.dima.ti.analysis;


import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

// sums up outgoing capacity of an airport for TM estimation
public class OutgoingMarginalsReducer implements GroupReduceFunction<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>,
        Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> {

    @Override
    public void reduce(Iterable<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> tuple7s,
                       Collector<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> out) throws Exception {
        boolean first = true;
        Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> result =
                new Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>();
        for (Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> t : tuple7s) {
            if (first) {
                result.f0 = t.f0;
                result.f1 = t.f1;
                result.f2 = t.f2;
                result.f3 = t.f3;
                result.f4 = t.f4;
                result.f5 = 0;
                result.f6 = 1.0; // Ki(0)
                result.f7 = true;
                first = false;
            }
            result.f5 += t.f5;
        }
        result.f5 = (int) Math.round(TrafficAnalysis.SLF * result.f5);
        out.collect(result);
    }
}

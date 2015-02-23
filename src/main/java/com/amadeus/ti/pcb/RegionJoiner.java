package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;

/**
 * add region info to geographical information
 */
public class RegionJoiner implements JoinFunction<Tuple7<String, String, String, String, String, Double, Double>,
        Tuple2<String, String>, Tuple7<String, String, String, String, String, Double, Double>> {

    @Override
    public Tuple7<String, String, String, String, String, Double, Double>
    join(Tuple7<String, String, String, String, String, Double, Double> first, Tuple2<String, String> second) throws Exception {
        first.f4 = second.f1;
        return first;
    }
}
package de.tuberlin.dima.ti.pcb;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;

/**
 * add region info to geographical information
 */
public class RegionJoiner implements JoinFunction<Tuple8<String, String, String, String, String, Double, Double, String>,
        Tuple2<String, String>, Tuple8<String, String, String, String, String, Double, Double, String>> {

    @Override
    public Tuple8<String, String, String, String, String, Double, Double, String>
    join(Tuple8<String, String, String, String, String, Double, Double, String> first, Tuple2<String, String> second) throws Exception {
        first.f4 = second.f1;
        return first;
    }
}
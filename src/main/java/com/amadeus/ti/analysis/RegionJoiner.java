package com.amadeus.ti.analysis;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

// add region to airport -> [region, country, state] mapping (join key is country)
public class RegionJoiner implements JoinFunction<Tuple4<String, String, String, String>,
        Tuple2<String, String>, Tuple4<String, String, String, String>> {

    @Override
    public Tuple4<String, String, String, String>
    join(Tuple4<String, String, String, String> first, Tuple2<String, String> second) throws Exception {
        first.f1 = second.f1;
        return first;
    }
}

package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class DefaultCapacityJoiner implements JoinFunction<Flight, Tuple2<String, Integer>, Flight> {

    @Override
    public Flight join(Flight first, Tuple2<String, Integer> second) throws Exception {
        first.setMaxCapacity(second.f1);
        return first;
    }
}
package com.amadeus.pcb.join;

import org.apache.flink.api.java.tuple.Tuple8;

public class ConnectionStats extends Tuple8<String, String, Integer, Integer, Integer, Integer, Integer, Integer> {

    public ConnectionStats() {super();}

    public ConnectionStats(String origin, String destination, int connectionCount, int maxCapacity, int minCapacity, int cumCapacity, int invalidCount, int legCount) {
        this.f0 = origin;
        this.f1 = destination;
        this.f2 = connectionCount;
        this.f3 = maxCapacity;
        this.f4 = minCapacity;
        this.f5 = cumCapacity;
        this.f6 = invalidCount;
        this.f7 = legCount;
    }

}

package com.amadeus.pcb.join;

import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Key;

import java.io.IOException;

public class ThreeLegJoinKey1 extends ThreeLegJoinKey{

    private String origin;
    private String destination;
    private String airline;
    private int flightNumber;
    private long departureTimestamp;

    public ThreeLegJoinKey1(Tuple2<
            Tuple13<String, String, String, Integer, Long, Long, Double, Double, String, Double, Double, String, String>,
            Tuple13<String, String, String, Integer, Long, Long, Double, Double, String, Double, Double, String, String>> tuple) {
        this.origin = tuple.f1.f0;
        this.destination = tuple.f1.f1;
        this.airline = tuple.f1.f2;
        this.flightNumber = tuple.f1.f3;
        this.departureTimestamp = tuple.f1.f4;
    }

    @Override
    public int compareTo(ThreeLegJoinKey threeLegJoinKey) {
        if(threeLegJoinKey == null) {
            throw new NullPointerException();
        }
        long result = 0L;
        result = this.origin.compareTo(threeLegJoinKey.origin);
        if(result != 0L) {
            return (int) result;
        }
        result = this.destination.compareTo(threeLegJoinKey.destination);
        if(result != 0L) {
            return (int) result;
        }
        result = this.airline.compareTo(threeLegJoinKey.airline);
        if(result != 0L) {
            return (int) result;
        }
        result = this.flightNumber - threeLegJoinKey.flightNumber;
        if(result != 0L) {
            return (int) result;
        }
        result = this.departureTimestamp - threeLegJoinKey.departureTimestamp;
        return (int) result;
    }
}
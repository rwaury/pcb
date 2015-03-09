package de.tuberlin.dima.old.join;

import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple2;

public class ThreeLegJoinKey0 extends ThreeLegJoinKey{

    public ThreeLegJoinKey0(Tuple2<
            Tuple13<String, String, String, Integer, Long, Long, Double, Double, String, Double, Double, String, String>,
            Tuple13<String, String, String, Integer, Long, Long, Double, Double, String, Double, Double, String, String>> tuple) {
        this.origin = tuple.f0.f0;
        this.destination = tuple.f0.f1;
        this.airline = tuple.f0.f2;
        this.flightNumber = tuple.f0.f3;
        this.departureTimestamp = tuple.f0.f4;
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
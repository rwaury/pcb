package com.amadeus.ti.analysis;

import org.apache.flink.api.java.tuple.Tuple19;

public class Itinerary extends Tuple19<String, String, String, String, String, String, String, String, Double, Double, Integer, Integer, Integer, Integer, Integer, Integer, Double, Double, String> {

    public Itinerary(){
        super();
    }

    public Itinerary(String origin, String destination, String day,
                     String flight1, String flight2, String flight3, String flight4, String flight5,
                     Double directDistance, Double travelDistance, Integer travelTime, Integer waitingTime,
                     Integer legCount, Integer lowerBound, Integer maxCapacity, Integer paxEstimate, Double paxEstimateModel, Double ODEstimate, String full) {
        this.f0 = origin;
        this.f1 = destination;
        this.f2 = day;
        this.f3 = flight1;
        this.f4 = flight2;
        this.f5 = flight3;
        this.f6 = flight4;
        this.f7 = flight5;
        this.f8 = directDistance;
        this.f9 = travelDistance;
        this.f10 = travelTime < 1 ? 1 : travelTime;
        this.f11 = waitingTime;
        this.f12 = legCount;
        this.f13 = lowerBound;
        this.f14 = maxCapacity;
        this.f15 = paxEstimate;
        this.f16 = paxEstimateModel;
        this.f17 = ODEstimate;
        this.f18 = full;
    }

    public Itinerary(Tuple19<String, String, String, String, String, String, String, String, Double, Double, Integer, Integer, Integer, Integer, Integer, Integer, Double, Double, String> tuple19) {
        this.f0 = tuple19.f0;
        this.f1 = tuple19.f1;
        this.f2 = tuple19.f2;
        this.f3 = tuple19.f3;
        this.f4 = tuple19.f4;
        this.f5 = tuple19.f5;
        this.f6 = tuple19.f6;
        this.f7 = tuple19.f7;
        this.f8 = tuple19.f8;
        this.f9 = tuple19.f9;
        this.f10 = tuple19.f10;
        this.f11 = tuple19.f11;
        this.f12 = tuple19.f12;
        this.f13 = tuple19.f13;
        this.f14 = tuple19.f14;
        this.f15 = tuple19.f15;
        this.f16 = tuple19.f16;
        this.f17 = tuple19.f17;
        this.f18 = tuple19.f18;
    }

    public Itinerary deepCopy() {
        return new Itinerary(this.copy());
    }

}

package com.amadeus.ti.analysis;

import org.apache.flink.api.java.tuple.Tuple20;

import java.util.HashSet;

public class Itinerary extends Tuple20<String, String, String, String, String, String, String, String, Double, Double, Integer, Integer, Integer, Integer, Integer, Integer, Double, Double, String, Integer> {

    public Itinerary(){
        super();
    }

    public Itinerary(String origin, String destination, String day,
                     String flight1, String flight2, String flight3, String flight4, String flight5,
                     Double directDistance, Double travelDistance, Integer travelTime, Integer waitingTime,
                     Integer legCount, Integer lowerBound, Integer maxCapacity, Integer paxEstimate, Double paxEstimateModel, Double ODEstimate, String full, Integer numCountries) {
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
        this.f19 = numCountries;
    }

    public Itinerary(Tuple20<String, String, String, String, String, String, String, String, Double, Double, Integer, Integer, Integer, Integer, Integer, Integer, Double, Double, String, Integer> tuple20) {
        this.f0 = tuple20.f0;
        this.f1 = tuple20.f1;
        this.f2 = tuple20.f2;
        this.f3 = tuple20.f3;
        this.f4 = tuple20.f4;
        this.f5 = tuple20.f5;
        this.f6 = tuple20.f6;
        this.f7 = tuple20.f7;
        this.f8 = tuple20.f8;
        this.f9 = tuple20.f9;
        this.f10 = tuple20.f10;
        this.f11 = tuple20.f11;
        this.f12 = tuple20.f12;
        this.f13 = tuple20.f13;
        this.f14 = tuple20.f14;
        this.f15 = tuple20.f15;
        this.f16 = tuple20.f16;
        this.f17 = tuple20.f17;
        this.f18 = tuple20.f18;
        this.f19 = tuple20.f19;
    }

    public Itinerary deepCopy() {
        return new Itinerary(this.copy());
    }

    public double getGeoDetour() {
        return Math.max(1.0, this.f9/this.f8);
    }

    public int getNumAirLines() {
        HashSet<String> airlines = new HashSet<String>(5);
        airlines.add(this.f3.substring(0,2));
        if(!this.f4.isEmpty()) {
            airlines.add(this.f4.substring(0,2));
        }
        if(!this.f5.isEmpty()) {
            airlines.add(this.f5.substring(0,2));
        }
        if(!this.f6.isEmpty()) {
            airlines.add(this.f6.substring(0,2));
        }
        if(!this.f7.isEmpty()) {
            airlines.add(this.f7.substring(0,2));
        }
        return Math.max(1, airlines.size());
    }

}

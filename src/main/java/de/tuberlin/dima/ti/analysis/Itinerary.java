package de.tuberlin.dima.ti.analysis;

import org.apache.flink.api.java.tuple.Tuple22;

import java.util.HashSet;

public class Itinerary extends Tuple22<String, String, String, String, String, String, String, String, Double, Double, Integer, Integer, Integer, Integer, Integer, Double, Double, Double, String, Integer, String, String> {

    public Itinerary(){
        super();
    }

    public Itinerary(String origin, String destination, String day,
                     String flight1, String flight2, String flight3, String flight4, String flight5,
                     Double directDistance, Double travelDistance, Integer travelTime, Integer waitingTime,
                     Integer legCount, Integer lowerBound, Integer maxCapacity, Double paxEstimate, Double paxEstimateModel, Double ODEstimate, String full, Integer numCountries,
                     String hub1, String hub2) {
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
        this.f20 = hub1;
        this.f21 = hub2;
    }

    public Itinerary(Tuple22<String, String, String, String, String, String, String, String, Double, Double, Integer, Integer, Integer, Integer, Integer, Double, Double, Double, String, Integer, String, String> tuple22) {
        this.f0 = tuple22.f0;
        this.f1 = tuple22.f1;
        this.f2 = tuple22.f2;
        this.f3 = tuple22.f3;
        this.f4 = tuple22.f4;
        this.f5 = tuple22.f5;
        this.f6 = tuple22.f6;
        this.f7 = tuple22.f7;
        this.f8 = tuple22.f8;
        this.f9 = tuple22.f9;
        this.f10 = tuple22.f10;
        this.f11 = tuple22.f11;
        this.f12 = tuple22.f12;
        this.f13 = tuple22.f13;
        this.f14 = tuple22.f14;
        this.f15 = tuple22.f15;
        this.f16 = tuple22.f16;
        this.f17 = tuple22.f17;
        this.f18 = tuple22.f18;
        this.f19 = tuple22.f19;
        this.f20 = tuple22.f20;
        this.f21 = tuple22.f21;
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

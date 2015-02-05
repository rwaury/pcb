package com.amadeus.pcb.join;

import org.apache.flink.api.java.tuple.Tuple16;

public class Itinerary extends Tuple16<String, String, String, String, String, String, String, String, Double, Double, Integer, Integer, Integer, Integer, Integer, String> {
    // o, d, day, dep, flight1, flight2, flight3, flight4, direct distance, travelled distance, travel time, waiting time, leg count, max cap, pax

    public Itinerary(){
        super();
    }

    public Itinerary(String origin, String destination, String day,
                     String flight1, String flight2, String flight3, String flight4, String flight5,
                     Double directDistance, Double travelDistance, Integer travelTime, Integer waitingTime,
                     Integer legCount, Integer maxCapacity, Integer pax, String full) {
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
        this.f10 = travelTime;
        this.f11 = waitingTime;
        this.f12 = legCount;
        this.f13 = maxCapacity;
        this.f14 = pax;
        this.f15 = full;
    }

}

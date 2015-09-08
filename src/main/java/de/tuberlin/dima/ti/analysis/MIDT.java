package de.tuberlin.dima.ti.analysis;

import org.apache.flink.api.java.tuple.Tuple16;

import java.util.HashSet;

public class MIDT extends Tuple16<String, String, String, String, String, String, String, String, Integer, Integer, Integer, Integer, Double, Integer, String, String> {

    public MIDT() {
        super();
    }

    public MIDT(String origin, String destination, String day,
                String flight1, String flight2, String flight3, String flight4, String flight5,
                Integer travelTime, Integer waitingTime, Integer legCount, Integer pax, Double geoDetour, Integer numCountries) {
        this.f0 = origin;
        this.f1 = destination;
        this.f2 = day;
        this.f3 = flight1;
        this.f4 = flight2;
        this.f5 = flight3;
        this.f6 = flight4;
        this.f7 = flight5;
        this.f8 = travelTime;
        this.f9 = waitingTime;
        this.f10 = legCount;
        this.f11 = pax;
        this.f12 = geoDetour;
        this.f13 = numCountries;
        this.f14 = "";
        this.f15 = "";
    }

    public MIDT(String origin, String destination, String day,
                String flight1, String flight2, String flight3, String flight4, String flight5,
                Integer travelTime, Integer waitingTime, Integer legCount, Integer pax, Double geoDetour, Integer numCountries,
                String hub1, String hub2) {
        this.f0 = origin;
        this.f1 = destination;
        this.f2 = day;
        this.f3 = flight1;
        this.f4 = flight2;
        this.f5 = flight3;
        this.f6 = flight4;
        this.f7 = flight5;
        this.f8 = travelTime;
        this.f9 = waitingTime;
        this.f10 = legCount;
        this.f11 = pax;
        this.f12 = geoDetour;
        this.f13 = numCountries;
        this.f14 = hub1;
        this.f15 = hub2;
    }

    public int getNumAirlines() {
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

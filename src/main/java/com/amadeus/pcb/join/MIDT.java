package com.amadeus.pcb.join;

import org.apache.flink.api.java.tuple.Tuple12;

/**
 * Created by robert on 02/02/15.
 */
public class MIDT extends Tuple12<String, String, String, String, String, String, String, String, Integer, Integer, Integer, Integer> {

    public MIDT() {
        super();
    }

    public MIDT(String origin, String destination, String day,
                String flight1, String flight2, String flight3, String flight4, String flight5,
                Integer travelTime, Integer waitingTime, Integer legCount, Integer pax) {
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
    }

}

package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Overwrites maximum capacity of a flight if more detailed data is available
 * for that aircraft - airline combination
 */
public class CapacityGrouper implements CoGroupFunction<Flight, Tuple3<String, String, Integer>, Flight> {

    @Override
    public void coGroup(Iterable<Flight> flights, Iterable<Tuple3<String, String, Integer>> cap, Collector<Flight> out) throws Exception {
        Iterator<Flight> flightIter = flights.iterator();
        Iterator<Tuple3<String, String, Integer>> capIter = cap.iterator();
        if(capIter.hasNext()) {
            Tuple3<String, String, Integer> newCap = capIter.next();
            while(flightIter.hasNext()) {
                Flight flight = flightIter.next();
                flight.setMaxCapacity(newCap.f2);
                out.collect(flight);
            }
            if(capIter.hasNext()) {
                throw new Exception("More than one capacity entry: " + capIter.next().toString());
            }
        } else {
            while(flightIter.hasNext()) {
                out.collect(flightIter.next());
            }
        }
    }
}
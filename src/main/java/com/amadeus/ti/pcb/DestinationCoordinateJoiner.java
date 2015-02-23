package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple7;

/**
 * add geographical data to Flight instances
 */
public class DestinationCoordinateJoiner implements JoinFunction<Flight, Tuple7<String, String, String, String, String, Double, Double>, Flight> {

    @Override
    public Flight join(Flight first, Tuple7<String, String, String, String, String, Double, Double> second)
            throws Exception {
        if (!second.f1.isEmpty()) {
            first.setDestinationCity(second.f1);
        } else {
            // use airport code as city code if city code unavailable
            first.setDestinationCity(second.f0);
        }
        first.setDestinationState(second.f2);
        first.setDestinationCountry(second.f3);
        first.setDestinationRegion(second.f4);
        first.setDestinationLatitude(second.f5);
        first.setDestinationLongitude(second.f6);
        return first;
    }

}
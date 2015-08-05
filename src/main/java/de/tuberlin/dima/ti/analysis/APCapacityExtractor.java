package de.tuberlin.dima.ti.analysis;

import de.tuberlin.dima.ti.pcb.Flight;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Date;

// parses one-leg flights produced by the CB and emits maximum capacities
public class APCapacityExtractor implements FlatMapFunction<Flight, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> {

    private boolean noPartition;
    private boolean DIPartition;
    private boolean fullPartition;

    public APCapacityExtractor(boolean noPartition, boolean DIPartition, boolean fullPartition) {
        this.noPartition = noPartition;
        this.DIPartition = DIPartition;
        this.fullPartition = fullPartition;
    }

    @Override
    public void flatMap(Flight flight, Collector<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> out) throws Exception {
        if (flight.getLegCount() > 1) {
            return;
        }
        if (flight.getDepartureTimestamp() > TrafficAnalysis.lastPossibleTimestamp ||
            flight.getDepartureTimestamp() < TrafficAnalysis.firstPossibleTimestamp) {
            return;
        }
        Date date = new Date(flight.getDepartureTimestamp());
        String dayString = TrafficAnalysis.dayFormat.format(date);
        boolean isInterRegional = !flight.getOriginRegion().equals(flight.getDestinationRegion());
        boolean isInternational = !flight.getOriginCountry().equals(flight.getDestinationCountry());
        boolean isInterState = true;
        if (!isInternational && TrafficAnalysis.countriesWithStates.contains(flight.getOriginCountry())) {
            isInterState = !flight.getOriginState().equals(flight.getDestinationState());
        }
        if(noPartition) {
            isInterRegional = false;
            isInternational = false;
            isInterState = false;
        }
        if(DIPartition) {
            isInterRegional = false;
            isInternational = isInternational;
            isInterState = false;
        }
        int capacity = flight.getMaxCapacity();
        Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> outgoing = new Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>
                (flight.getOriginAirport(), dayString, isInterRegional, isInternational, isInterState, capacity, 0);
        Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> incoming = new Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>
                (flight.getDestinationAirport(), dayString, isInterRegional, isInternational, isInterState, 0, capacity);
        out.collect(incoming);
        out.collect(outgoing);
    }
}

package com.amadeus.ti.analysis;

import com.amadeus.ti.pcb.Flight;
import com.amadeus.ti.pcb.GeoInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

// replaces all points outside of the US with a fictious point on the opposite side of the
// of the approximated mean center of the US population
// and also drops all flights that are not at least partially in the US
public class GeoInfoReplacer {

    public static GeoInfo XXX = new GeoInfo(TrafficAnalysis.NON_US_POINT, "", TrafficAnalysis.NON_US_CITY,
            TrafficAnalysis.NON_US_STATE, TrafficAnalysis.NON_US_COUNTRY, TrafficAnalysis.NON_US_REGION,
            TrafficAnalysis.NON_US_LATITUDE, TrafficAnalysis.NON_US_LONGITUDE, TrafficAnalysis.NON_US_ICAO);

    public static class GeoInfoReplacerUS1 implements FlatMapFunction<Flight, Flight> {

        @Override
        public void flatMap(Flight flight, Collector<Flight> out) throws Exception {
            if(!(flight.getOriginCountry().equals("US") || flight.getDestinationCountry().equals("US"))) {
                return;
            } else if(!flight.getOriginCountry().equals("US")) {
                flight.f0 = XXX.clone();
                flight.f10 = XXX.clone();
                out.collect(flight);
            } else if(!flight.getDestinationCountry().equals("US")) {
                flight.f2 = XXX.clone();
                out.collect(flight);
            } else {
                out.collect(flight);
            }
        }
    }


    public static class GeoInfoReplacerUS2 implements FlatMapFunction<Tuple2<Flight, Flight>, Tuple2<Flight, Flight>> {

        @Override
        public void flatMap(Tuple2<Flight, Flight> flight, Collector<Tuple2<Flight, Flight>> out) throws Exception {
            if(!flight.f0.getDestinationCountry().equals("US")) {
                return;
            }
            if(!flight.f0.getOriginCountry().equals("US")) {
                flight.f0.f0 = XXX.clone();
                flight.f0.f10 = XXX.clone();
            }
            if(!flight.f0.getDestinationCountry().equals("US")) {
                flight.f0.f2 = XXX.clone();
            }
            if(!flight.f1.getOriginCountry().equals("US")) {
                flight.f1.f0 = XXX.clone();
                flight.f1.f10 = XXX.clone();
            }
            if(!flight.f1.getDestinationCountry().equals("US")) {
                flight.f1.f2 = XXX.clone();
            }
            out.collect(flight);
        }
    }


    public static class GeoInfoReplacerUS3 implements FlatMapFunction<Tuple3<Flight, Flight, Flight>, Tuple3<Flight, Flight, Flight>> {

        @Override
        public void flatMap(Tuple3<Flight, Flight, Flight> flight, Collector<Tuple3<Flight, Flight, Flight>> out) throws Exception {
            if(!(flight.f0.getDestinationCountry().equals("US") && flight.f1.getDestinationCountry().equals("US"))) {
                return;
            } else if(!(flight.f0.getOriginCountry().equals("US") && flight.f2.getDestinationCountry().equals("US"))) {
                return;
            }
            if(!flight.f0.getOriginCountry().equals("US")) {
                flight.f0.f0 = XXX.clone();
                flight.f0.f10 = XXX.clone();
            }
            if(!flight.f0.getDestinationCountry().equals("US")) {
                flight.f0.f2 = XXX.clone();
            }
            if(!flight.f1.getOriginCountry().equals("US")) {
                flight.f1.f0 = XXX.clone();
                flight.f1.f10 = XXX.clone();
            }
            if(!flight.f1.getDestinationCountry().equals("US")) {
                flight.f1.f2 = XXX.clone();
            }
            if(!flight.f2.getOriginCountry().equals("US")) {
                flight.f2.f0 = XXX.clone();
                flight.f2.f10 = XXX.clone();
            }
            if(!flight.f2.getDestinationCountry().equals("US")) {
                flight.f2.f2 = XXX.clone();
            }
            out.collect(flight);
        }
    }


}

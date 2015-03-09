package com.amadeus.ti.analysis;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;

public class MIDTCapacityEmitter extends RichFlatMapFunction<String, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> {

    private HashMap<String, String> APToRegion;
    private HashMap<String, String> APToCountry;
    private HashMap<String, String> APToState;



    @Override
    public void open(Configuration parameters) {
        Collection<Tuple8<String, String, String, String, String, Double, Double, String>> broadcastSet =
                this.getRuntimeContext().getBroadcastVariable(TrafficAnalysis.AP_GEO_DATA);
        this.APToRegion = new HashMap<String, String>(broadcastSet.size());
        this.APToCountry = new HashMap<String, String>(broadcastSet.size());
        this.APToState = new HashMap<String, String>(200);
        for(Tuple8<String, String, String, String, String, Double, Double, String> t : broadcastSet) {
            this.APToRegion.put(t.f0, t.f4);
            this.APToCountry.put(t.f0, t.f3);
            if(TrafficAnalysis.countriesWithStates.contains(t.f3)) {
                this.APToState.put(t.f0, t.f2);
            }
        }
    }

    @Override
    public void flatMap(String s, Collector<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> out) throws Exception {
        if(s.startsWith("$")) {
            return;
        }
        String[] tmp = s.split(";");
        if(tmp.length < 15) {
            return;
        }
        int pax = Integer.parseInt(tmp[2].trim());
        int segmentCount = Integer.parseInt(tmp[3].trim());
        long departureDay = Long.parseLong(tmp[11].trim())-1;
        long departureTimestamp = TrafficAnalysis.firstPossibleTimestamp + (departureDay*24L*60L*60L*1000L);
        Date date = new Date(departureTimestamp);
        String dayString = TrafficAnalysis.dayFormat.format(date);
        if(departureDay > 6) {
            throw new Exception("Value error: " + s);
        }
        boolean isIntercontinental = false;
        boolean isInternational = false;
        boolean isInterstate = true;
        int offset = 9;
        int counter = 0;
        int index = 6;
        while(counter < segmentCount) {
            String apOut = tmp[index].trim();
            String apIn = tmp[index+1].trim();
            index += offset;
            counter++;
            String outRegion = APToRegion.get(apOut);
            String inRegion = APToRegion.get(apIn);
            String outCountry = APToCountry.get(apOut);
            String inCountry = APToCountry.get(apIn);
            if(outCountry != null && inCountry != null) {
                if(TrafficAnalysis.US_ONLY) {
                    if(!inCountry.equals("US")) {
                        apIn = TrafficAnalysis.NON_US_POINT;
                        inCountry = TrafficAnalysis.NON_US_COUNTRY;
                        inRegion = TrafficAnalysis.NON_US_REGION;
                    }
                    if(!outCountry.equals("US")) {
                        apOut = TrafficAnalysis.NON_US_POINT;
                        outCountry = TrafficAnalysis.NON_US_COUNTRY;
                        outRegion = TrafficAnalysis.NON_US_REGION;
                    }
                }
                isIntercontinental = !outRegion.equals(inRegion);
                isInternational = !outCountry.equals(inCountry);
                isInterstate = true;
                if(!isInternational && TrafficAnalysis.countriesWithStates.contains(outCountry)) {
                    String outState = APToState.get(apOut);
                    String inState = APToState.get(apIn);
                    isInterstate = !outState.equals(inState);
                }
            } else {
                continue;
            }
            Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> tOut =
                    new Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>(apOut, dayString, isIntercontinental, isInternational, isInterstate, pax, 0);
            Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> tIn =
                    new Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>(apIn, dayString, isIntercontinental, isInternational, isInterstate, 0, pax);
            out.collect(tOut);
            out.collect(tIn);
        }
    }
}
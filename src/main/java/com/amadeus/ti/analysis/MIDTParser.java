package com.amadeus.ti.analysis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

// MIDT file to MIDT tuple
public class MIDTParser implements FlatMapFunction<String, MIDT> {

    private final static String HEADER = "$";

    SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

    @Override
    public void flatMap(String s, Collector<MIDT> out) throws Exception {
        if(s.startsWith(HEADER)) {
            return;
        }
        String[] tmp = s.split(";");
        if(tmp.length < 15) {
            return;
        }
        String origin = tmp[0].trim();
        String destination = tmp[1].trim();
        int pax = Integer.parseInt(tmp[2].trim());
        int segmentCount = Integer.parseInt(tmp[3].trim());
        String flight1 = tmp[9].trim() + tmp[10].replaceAll("[^0-9]", "");
        String flight2 = "";
        String flight3 = "";
        String flight4 = "";
        String flight5 = "";
        long departureDay = Long.parseLong(tmp[11].trim())-1;
        if(departureDay > 6) {
            throw new Exception("Value error: " + s);
        }
        int departure = Integer.parseInt(tmp[12].trim());
        int arrival = Integer.parseInt(tmp[14].trim());
        int waitingTime = 0;
        int tmpDep = 0;
        if(segmentCount > 1) {
            flight2 = tmp[18].trim() + tmp[19].replaceAll("[^0-9]", "");
            tmpDep = Integer.parseInt(tmp[21].trim());
            waitingTime += tmpDep - arrival;
            arrival = Integer.parseInt(tmp[23].trim());
        }
        if(segmentCount > 2) {
            flight3 = tmp[27].trim() + tmp[28].replaceAll("[^0-9]", "");
            tmpDep = Integer.parseInt(tmp[30].trim());
            waitingTime += tmpDep - arrival;
            arrival = Integer.parseInt(tmp[32].trim());
        }
        if(segmentCount > 3) {
            flight4 = tmp[36].trim() + tmp[37].replaceAll("[^0-9]", "");
            tmpDep = Integer.parseInt(tmp[39].trim());
            waitingTime += tmpDep - arrival;
            arrival = Integer.parseInt(tmp[41].trim());
        }
        if(segmentCount > 4) {
            flight5 = tmp[45].trim() + tmp[46].replaceAll("[^0-9]", "");
            tmpDep = Integer.parseInt(tmp[48].trim());
            waitingTime += tmpDep - arrival;
            arrival = Integer.parseInt(tmp[49].trim());
        }
        int travelTime = arrival - departure;
        if(travelTime < 1) {
            return;
        }
        long departureTimestamp = TrafficAnalysis.firstPossibleTimestamp + (departureDay*24L*60L*60L*1000L) + 1L;
        Date date = new Date(departureTimestamp);
        String dayString = format.format(date);
        MIDT result = new MIDT(origin, destination, dayString,
                flight1, flight2, flight3, flight4, flight5, travelTime, waitingTime,
                segmentCount, pax);
        out.collect(result);
    }
}
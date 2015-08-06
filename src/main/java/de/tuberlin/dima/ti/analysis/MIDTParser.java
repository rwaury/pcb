package de.tuberlin.dima.ti.analysis;

import de.tuberlin.dima.ti.pcb.CBUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

// MIDT file to MIDT tuple
public class MIDTParser extends RichFlatMapFunction<String, MIDT> {

    private final static String HEADER = "$";

    private HashMap<String, AirportInfo> airports;

    @Override
    public void open(Configuration parameters) {
        Collection<Tuple8<String, String, String, String, String, Double, Double, String>> broadcastSet =
                this.getRuntimeContext().getBroadcastVariable(TrafficAnalysis.AP_GEO_DATA);
        this.airports = new HashMap<String, AirportInfo>(broadcastSet.size());
        for(Tuple8<String, String, String, String, String, Double, Double, String> tuple8 : broadcastSet) {
            this.airports.put(tuple8.f0, new AirportInfo(tuple8));
        }
    }


    @Override
    public void flatMap(String s, Collector<MIDT> out) throws Exception {
        if(s.startsWith(HEADER)) {
            return;
        }
        String[] tmp = s.split(";");
        if(tmp.length < 15) {
            return;
        }
        HashSet<String> countries = new HashSet<String>(5);

        String origin = tmp[0].trim();
        String destination = tmp[1].trim();
        AirportInfo o = airports.get(origin);
        AirportInfo d = airports.get(destination);
        if(o == null || d == null) {
            return;
        }
        double directDistance = CBUtil.dist(o.latitude, o.longitude, d.latitude, d.longitude);

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

        String ap1 = tmp[6].trim();
        String ap2 = tmp[7].trim();
        o = airports.get(ap1);
        d = airports.get(ap2);
        if(o == null || d == null) {
            return;
        }
        double travelledDistance = CBUtil.dist(o.latitude, o.longitude, d.latitude, d.longitude);
        countries.add(o.country);
        countries.add(d.country);

        int departure = Integer.parseInt(tmp[12].trim());
        int arrival = Integer.parseInt(tmp[14].trim());
        int waitingTime = 0;
        int tmpDep = 0;

        if(segmentCount > 1) {
            flight2 = tmp[18].trim() + tmp[19].replaceAll("[^0-9]", "");
            tmpDep = Integer.parseInt(tmp[21].trim());
            waitingTime += tmpDep - arrival;
            arrival = Integer.parseInt(tmp[23].trim());
            ap1 = tmp[15].trim();
            ap2 = tmp[16].trim();
            o = airports.get(ap1);
            d = airports.get(ap2);
            if(o == null || d == null) {
                return;
            }
            travelledDistance += CBUtil.dist(o.latitude, o.longitude, d.latitude, d.longitude);
            countries.add(o.country);
            countries.add(d.country);
        }
        if(segmentCount > 2) {
            flight3 = tmp[27].trim() + tmp[28].replaceAll("[^0-9]", "");
            tmpDep = Integer.parseInt(tmp[30].trim());
            waitingTime += tmpDep - arrival;
            arrival = Integer.parseInt(tmp[32].trim());
            ap1 = tmp[24].trim();
            ap2 = tmp[25].trim();
            o = airports.get(ap1);
            d = airports.get(ap2);
            if(o == null || d == null) {
                return;
            }
            travelledDistance += CBUtil.dist(o.latitude, o.longitude, d.latitude, d.longitude);
            countries.add(o.country);
            countries.add(d.country);
        }
        if(segmentCount > 3) {
            flight4 = tmp[36].trim() + tmp[37].replaceAll("[^0-9]", "");
            tmpDep = Integer.parseInt(tmp[39].trim());
            waitingTime += tmpDep - arrival;
            arrival = Integer.parseInt(tmp[41].trim());
            ap1 = tmp[33].trim();
            ap2 = tmp[34].trim();
            o = airports.get(ap1);
            d = airports.get(ap2);
            if(o == null || d == null) {
                return;
            }
            travelledDistance += CBUtil.dist(o.latitude, o.longitude, d.latitude, d.longitude);
            countries.add(o.country);
            countries.add(d.country);
        }
        if(segmentCount > 4) {
            flight5 = tmp[45].trim() + tmp[46].replaceAll("[^0-9]", "");
            tmpDep = Integer.parseInt(tmp[48].trim());
            waitingTime += tmpDep - arrival;
            arrival = Integer.parseInt(tmp[49].trim());
            ap1 = tmp[42].trim();
            ap2 = tmp[43].trim();
            o = airports.get(ap1);
            d = airports.get(ap2);
            if(o == null || d == null) {
                return;
            }
            travelledDistance += CBUtil.dist(o.latitude, o.longitude, d.latitude, d.longitude);
            countries.add(o.country);
            countries.add(d.country);
        }
        int travelTime = arrival - departure;
        if(travelTime < 1) {
            return;
        }
        long departureTimestamp = TrafficAnalysis.firstPossibleTimestamp + (departureDay*24L*60L*60L*1000L) + 1L;
        Date date = new Date(departureTimestamp);
        String dayString = TrafficAnalysis.dayFormat.format(date);
        double geoDetour = Math.max(1.0, travelledDistance/directDistance);
        MIDT result = new MIDT(origin, destination, dayString,
                flight1, flight2, flight3, flight4, flight5, travelTime, waitingTime,
                segmentCount, pax, geoDetour, Math.max(1, countries.size()));
        out.collect(result);
    }

    private class AirportInfo {

        String iata;
        String city;
        String state;
        String country;
        String region;
        double latitude;
        double longitude;
        String icao;

        public AirportInfo(Tuple8<String, String, String, String, String, Double, Double, String> tuple8) {
            this.iata = tuple8.f0;
            this.city = tuple8.f1;
            this.state = tuple8.f2;
            this.country = tuple8.f3;
            this.region = tuple8.f4;
            this.latitude = tuple8.f5;
            this.longitude = tuple8.f6;
            this.icao = tuple8.f7;
        }
    }
}
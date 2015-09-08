package de.tuberlin.dima.ti.analysis;

import de.tuberlin.dima.ti.pcb.CBUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.HashMap;
import java.util.Random;

/**
 * Parser for preprocessed DB1BMarket data in CSV format
 * that discards some of the records and emits them as MIDT tuples
 */
public class DB1B2MIDTParser extends RichFlatMapFunction<String, MIDT> {

    private static final String[] DAYSTRINGS = {"05052014", "06052014", "07052014", "08052014", "09052014", "10052014", "11052014"};

    private static final String SEPARATOR = ",";

    private static final String SEPARATOR2 = ",";

    private HashMap<String, AirportInfo> airportsInfo;

    private Random rng;

    private static final double AVG_CRUISE_SPEED = 800.0;

    private static final int TL_OVERHEAD = 20;

    private static final int AVG_WAITING_TIME = 60;

    // if true randomly assigns day of the week, otherwise emits one MIDT entry for each day
    private boolean spreadOverWeek;

    public DB1B2MIDTParser(boolean spreadOverWeek) {
        this.spreadOverWeek = spreadOverWeek;
    }

    @Override
    public void open(Configuration parameters) {
        Collection<Tuple8<String, String, String, String, String, Double, Double, String>> broadcastSet =
                this.getRuntimeContext().getBroadcastVariable(TrafficAnalysis.AP_GEO_DATA);
        this.airportsInfo = new HashMap<String, AirportInfo>(broadcastSet.size());
        for(Tuple8<String, String, String, String, String, Double, Double, String> tuple8 : broadcastSet) {
            this.airportsInfo.put(tuple8.f0, new AirportInfo(tuple8));
        }
        if(spreadOverWeek) {
            this.rng = new Random(System.currentTimeMillis());
        }
    }

    @Override
    public void flatMap(String s, Collector<MIDT> collector) throws Exception {
        String[] tmp = s.split(SEPARATOR);
        String[] airports = tmp[0].split(SEPARATOR2);
        String[] airlines = tmp[1].split(SEPARATOR2);
        Integer pax = Integer.parseInt(tmp[2].trim());
        if(airports.length > 4 || airports.length < 2) {
            return;
        }
        if(airports.length != airlines.length+1) {
            // should be impossible
            return;
        }
        if(pax < 1) {
            return;
        }
        String origin = airports[0].trim();
        if(!this.airportsInfo.containsKey(origin)) {
            return;
        }
        String hub1 = "";
        String hub2 = "";
        String destination = "";
        String al1 = airlines[0].trim();
        String al2 = "";
        String al3 = "";
        int travelTime = 1;
        int waitingTime = 0;
        int legCount = 1;
        double geoDetour = 1.0;
        int numCountries = 1;
        if(airports.length == 2) {
            destination = airports[1].trim();
            if(!this.airportsInfo.containsKey(destination)) {
                return;
            }
            travelTime = estimateTravelTime(origin, destination);
        } else if(airports.length == 3) {
            hub1 = airports[1].trim();
            destination = airports[2].trim();
            if(!this.airportsInfo.containsKey(hub1) ||
               !this.airportsInfo.containsKey(destination)) {
                return;
            }
            al2 = airlines[1].trim();
            travelTime = estimateTravelTime(origin, hub1, destination);
            waitingTime = AVG_WAITING_TIME;
            legCount = 2;
            geoDetour = getGeoDetour(origin, hub1, destination);
        } else if(airports.length == 4) {
            hub1 = airports[1].trim();
            hub2 = airports[2].trim();
            destination = airports[3].trim();
            if(!this.airportsInfo.containsKey(hub1) ||
               !this.airportsInfo.containsKey(hub2) ||
               !this.airportsInfo.containsKey(destination)) {
                return;
            }
            al2 = airlines[1].trim();
            al3 = airlines[2].trim();
            travelTime = estimateTravelTime(origin, hub1, hub2, destination);
            waitingTime = 2*AVG_WAITING_TIME;
            legCount = 3;
            geoDetour = getGeoDetour(origin, hub1, hub2, destination);
        } else {
            return;
        }
        if(spreadOverWeek) {
            int randomDay = rng.nextInt(DAYSTRINGS.length);
            String dayString = DAYSTRINGS[randomDay];
            collector.collect(new MIDT(origin, destination, dayString, al1, al2, al3, "", "", travelTime, waitingTime, legCount, pax, geoDetour, numCountries, hub1, hub2));
        } else {
            for (int i = 0; i < DAYSTRINGS.length; i++) {
                String dayString = DAYSTRINGS[i];
                collector.collect(new MIDT(origin, destination, dayString, al1, al2, al3, "", "", travelTime, waitingTime, legCount, pax, geoDetour, numCountries, hub1, hub2));
            }
        }
    }

    private int estimateTravelTime(String origin, String destination) {
        AirportInfo o = airportsInfo.get(origin);
        AirportInfo d = airportsInfo.get(destination);
        double directDistance = CBUtil.dist(o.latitude, o.longitude, d.latitude, d.longitude);
        int flightInMinutes = (int)((directDistance*60.0)/AVG_CRUISE_SPEED);
        return flightInMinutes + TL_OVERHEAD;
    }

    private int estimateTravelTime(String origin, String hub1, String destination) {
        AirportInfo o = airportsInfo.get(origin);
        AirportInfo h = airportsInfo.get(hub1);
        AirportInfo d = airportsInfo.get(destination);
        double leg1 = CBUtil.dist(o.latitude, o.longitude, h.latitude, h.longitude);
        double leg2 = CBUtil.dist(h.latitude, h.longitude, d.latitude, d.longitude);
        int leg1InMinutes = (int)((leg1*60.0)/AVG_CRUISE_SPEED);
        int leg2InMinutes = (int)((leg2*60.0)/AVG_CRUISE_SPEED);
        return leg1InMinutes + TL_OVERHEAD + AVG_WAITING_TIME + leg2InMinutes + TL_OVERHEAD;
    }

    private int estimateTravelTime(String origin, String hub1, String hub2, String destination) {
        AirportInfo o = airportsInfo.get(origin);
        AirportInfo h1 = airportsInfo.get(hub1);
        AirportInfo h2 = airportsInfo.get(hub2);
        AirportInfo d = airportsInfo.get(destination);
        double leg1 = CBUtil.dist(o.latitude, o.longitude, h1.latitude, h1.longitude);
        double leg2 = CBUtil.dist(h1.latitude, h1.longitude, h2.latitude, h2.longitude);
        double leg3 = CBUtil.dist(h2.latitude, h2.longitude, d.latitude, d.longitude);
        int leg1InMinutes = (int)((leg1*60.0)/AVG_CRUISE_SPEED);
        int leg2InMinutes = (int)((leg2*60.0)/AVG_CRUISE_SPEED);
        int leg3InMinutes = (int)((leg3*60.0)/AVG_CRUISE_SPEED);
        return leg1InMinutes + TL_OVERHEAD + AVG_WAITING_TIME + leg2InMinutes + TL_OVERHEAD + AVG_WAITING_TIME + leg3InMinutes + TL_OVERHEAD;
    }

    private double getGeoDetour(String origin, String hub1, String destination) {
        AirportInfo o = airportsInfo.get(origin);
        AirportInfo h = airportsInfo.get(hub1);
        AirportInfo d = airportsInfo.get(destination);
        double directDistance = CBUtil.dist(o.latitude, o.longitude, d.latitude, d.longitude);
        double leg1 = CBUtil.dist(o.latitude, o.longitude, h.latitude, h.longitude);
        double leg2 = CBUtil.dist(h.latitude, h.longitude, d.latitude, d.longitude);
        return (leg1+leg2)/directDistance;
    }

    private double getGeoDetour(String origin, String hub1, String hub2, String destination) {
        AirportInfo o = airportsInfo.get(origin);
        AirportInfo h1 = airportsInfo.get(hub1);
        AirportInfo h2 = airportsInfo.get(hub2);
        AirportInfo d = airportsInfo.get(destination);
        double directDistance = CBUtil.dist(o.latitude, o.longitude, d.latitude, d.longitude);
        double leg1 = CBUtil.dist(o.latitude, o.longitude, h1.latitude, h1.longitude);
        double leg2 = CBUtil.dist(h1.latitude, h1.longitude, h2.latitude, h2.longitude);
        double leg3 = CBUtil.dist(h2.latitude, h2.longitude, d.latitude, d.longitude);
        return (leg1+leg2+leg3)/directDistance;
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

package com.amadeus.pcb.join;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

public class FlightConnectionJoiner {

    public static final Logger LOG = LoggerFactory.getLogger(FlightConnectionJoiner.class);

    private static String schedulePath = "hdfs:///user/rwaury/input2/all_catalog_140410.txt";
    private static String oriPath = "hdfs:///user/rwaury/input2/ori_por_public.csv";
    private static String regionPath = "hdfs:///user/rwaury/input2/ori_country_region_info.csv";
    private static String defaultCapacityPath = "hdfs:///user/rwaury/input2/default_capacities.csv";
    private static String capacityPath = "hdfs:///user/rwaury/input2/capacities_2014-07-01.csv";
    private static String mctPath = "hdfs:///user/rwaury/input2/mct.csv";
    private static String outputPath = "hdfs:///user/rwaury/output2/flights/";

    public static final long START = 1398902400000L;//1398902400000L;
    public static final long END = 1401580800000L;//1399507200000L;//1399020800000L;//

    public static final long WEEK_START = 1399248000000L;
    public static final long WEEK_END = 1399852800000L;

    public static final long MCT_MIN = 10L;
    //public static final long MCT_MAX = 551L;

    public static final Character EMPTY = new Character(' ');

    public static final long WINDOW_SIZE = 48L * 60L * 60L * 1000L; // 48 hours
    public static final int MAX_WINDOW_ID = (int) Math.ceil((2.0 * (END - START)) / (double) WINDOW_SIZE);

    public static int getFirstWindow(long timestamp) {
        int windowIndex = (int) (2L * (timestamp - START) / WINDOW_SIZE);
        return windowIndex;
    }

    public static int getSecondWindow(long timestamp) {
        return getFirstWindow(timestamp) + 1;
    }

    // Tuple19< String,         String,         String,         String,         Double,     Double,     Long,
    //          originAirport,  originTerminal, originCity,     originCountry,  originLat,  originLong, departureTimestamp,
    //          String,         String,         String,         String,         Double,     Double,     Long,
    //          destAirport,    destTerminal,   destCity,       destCountry,    destLat,    destLong,   arrivalTimestamp,
    //          String,     Integer,        String,         Integer,        String      >
    //          airline,    flightNumber,   aircraftType,   maxCapacity,    codeshareInfo
    // Tuple19<String, String, String, String, Double, Double, Long, String, String, String, String, Double, Double, Long, String, Integer, String, Integer, String>

    //Tuple12<String, String, String, Integer, Long, Long, Double, Double, String, Double, Double, String, String , String, String>
    //origin, destination, airline, flight number, departure, arrival, originLat, originLong, originCountry, destinationLat, destinationLong,
    //destinationCountry, originTerminal, destinationTerminal, codeshareInfo

    @SuppressWarnings("serial")
    public static class FilteringUTCExtractor implements
            FlatMapFunction<String, Flight> {

        String[] tmp = null;

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+0000"), Locale.FRENCH);

        SimpleDateFormat format = new SimpleDateFormat("yyMMddHHmm");

        Date earliest = new Date(START);
        Date latest = new Date(END);

        private final static ArrayList<String> nonAirCraftList = new ArrayList<String>() {{
            add("AGH");
            add("BH2");
            add("BUS");
            add("ICE");
            add("LCH");
            add("LMO");
            add("MD9");
            add("NDE");
            add("S61");
            add("S76");
            add("TRN");
            add("TSL");
        }};

        public void flatMap(String value, Collector<Flight> out) throws Exception {
            // TODO: check for service type SSIM p.483
            tmp = value.split("\\^");
            if (tmp[0].trim().startsWith("#")) {
                // header
                return;
            }
            String aircraft = tmp[9].trim();
            if (nonAirCraftList.contains(aircraft)) {
                // not an aircraft
                return;
            }
            String localDepartureDate = tmp[0].trim();
            String localDepartureTime = tmp[3].trim();
            String localDepartureOffset = tmp[4].trim();
            Date departure = format.parse(localDepartureDate + localDepartureTime);
            cal.setTime(departure);
            int sign = 0;
            if (localDepartureOffset.startsWith("-")) {
                sign = 1;
            } else if (localDepartureOffset.startsWith("+")) {
                sign = -1;
            } else {
                throw new Exception("Parse error. Wrong sign! Original String: " + value);
            }
            int hours = Integer.parseInt(localDepartureOffset.substring(1, 3));
            int minutes = Integer.parseInt(localDepartureOffset.substring(3, 5));
            cal.add(Calendar.HOUR_OF_DAY, sign * hours);
            cal.add(Calendar.MINUTE, sign * minutes);
            departure = cal.getTime();
            if (departure.before(earliest) || departure.after(latest)) {
                return;
            }
            if (tmp.length > 37 && !tmp[37].trim().isEmpty()) {
                // not an operating carrier
                return;
            }
            String localArrivalTime = tmp[5].trim();
            String localArrivalOffset = tmp[6].trim();
            String dateChange = tmp[7].trim();
            Date arrival = format.parse(localDepartureDate + localArrivalTime);
            cal.setTime(arrival);
            if (!dateChange.equals("0")) {
                if (dateChange.equals("1")) {
                    cal.add(Calendar.DAY_OF_YEAR, 1);
                } else if (dateChange.equals("2")) {
                    cal.add(Calendar.DAY_OF_YEAR, 2);
                } else if (dateChange.equals("A") || dateChange.equals("J") || dateChange.equals("-1")) {
                    cal.add(Calendar.DAY_OF_YEAR, -1);
                } else {
                    throw new Exception("Unknown arrival_date_variation modifier: " + dateChange + " original string: " + value);
                }
            }
            if (localArrivalOffset.startsWith("-")) {
                sign = 1;
            } else if (localArrivalOffset.startsWith("+")) {
                sign = -1;
            } else {
                throw new Exception("Parse error. Wrong sign!");
            }
            hours = Integer.parseInt(localArrivalOffset.substring(1, 3));
            minutes = Integer.parseInt(localArrivalOffset.substring(3, 5));
            cal.add(Calendar.HOUR_OF_DAY, sign * hours);
            cal.add(Calendar.MINUTE, sign * minutes);
            arrival = cal.getTime();
            // sanity check
            if (arrival.before(departure) || arrival.equals(departure)) {
                return;
                /*throw new Exception("Sanity check failed! Arrival equal to or earlier than departure.\n" +
						"Departure: " + departure.toString() + " Arrival: " + arrival.toString() + "\n"  + 
						"Sign: " + sign + " Hours: " + hours + " Minutes: " + minutes + "\n" +
						"Original value: " + value);*/
            }
            String codeshareInfo = "";
            if (tmp.length > 36) {
                codeshareInfo = tmp[36].trim();
                if (codeshareInfo.length() > 2) {
                    // first two letters don't contain flight information
                    codeshareInfo = codeshareInfo.substring(2);
                }
            }
            String originAirport = tmp[1].trim();
            String originTerminal = tmp[19].trim();
            String destinationAirport = tmp[2].trim();
            String destinationTerminal = tmp[20].trim();
            String airline = tmp[13].trim();
            Character trafficRestriction = EMPTY;
            if (!tmp[12].trim().isEmpty()) {
                trafficRestriction = tmp[12].trim().charAt(0);
            }
            Integer flightNumber = Integer.parseInt(tmp[14].trim());
            out.collect(new Flight(originAirport, originTerminal, "", "", "", "", 0.0, 0.0, departure.getTime(),
                    destinationAirport, destinationTerminal, "", "", "", "", 0.0, 0.0, arrival.getTime(),
                    airline, flightNumber, aircraft, -1, codeshareInfo, trafficRestriction));
        }
    }

    @SuppressWarnings("serial")
    public static class AirportCoordinateExtractor implements
            FlatMapFunction<String, Tuple7<String, String, String, String, String, Double, Double>> {

        String[] tmp = null;
        String from = null;
        String until = null;

        @Override
        public void flatMap(String value, Collector<Tuple7<String, String, String, String, String, Double, Double>> out) throws Exception {
            tmp = value.split("\\^");
            if (tmp[0].trim().startsWith("#")) {
                // header
                return;
            }
            if (!tmp[41].trim().equals("A")) {
                // not an airport
                return;
            }
            from = tmp[13].trim();
            until = tmp[14].trim();
            if (!until.equals("")) {
                // expired
                return;
            }
            Double latitude;
            Double longitude;
            try {
                latitude = new Double(Double.parseDouble(tmp[8].trim()));
                longitude = new Double(Double.parseDouble(tmp[9].trim()));
            } catch (Exception e) {
                // invalid coordinates
                return;
            }
            String iataCode = tmp[0].trim();
            String cityCode = tmp[36].trim();
            String countryCode = tmp[16].trim();
            String stateCode = tmp[40].trim();
            out.collect(new Tuple7<String, String, String, String, String, Double, Double>(iataCode, cityCode, stateCode, countryCode, "", latitude, longitude));
        }
    }

    public static class RegionExtractor implements MapFunction<String, Tuple2<String, String>> {

        String[] tmp = null;

        @Override
        public Tuple2<String, String> map(String s) throws Exception {
            tmp = s.split("\\^");
            String countryCode = tmp[0].trim().replace("\"", "");
            String regionCode = "";
            if (tmp[8].trim().equals("1")) {
                regionCode = "SCH";
            } else {
                regionCode = tmp[6].trim().replace("\"", "");
            }
            return new Tuple2<String, String>(countryCode, regionCode);
        }
    }

    public static class RegionJoiner implements JoinFunction<Tuple7<String, String, String, String, String, Double, Double>,
            Tuple2<String, String>, Tuple7<String, String, String, String, String, Double, Double>> {

        @Override
        public Tuple7<String, String, String, String, String, Double, Double>
        join(Tuple7<String, String, String, String, String, Double, Double> first, Tuple2<String, String> second) throws Exception {
            first.f4 = second.f1;
            return first;
        }
    }

    @SuppressWarnings("serial")
    public static class OriginCoordinateJoiner implements JoinFunction<Flight, Tuple7<String, String, String, String, String, Double, Double>, Flight> {

        @Override
        public Flight join(Flight first, Tuple7<String, String, String, String, String, Double, Double> second)
                throws Exception {
            if (!second.f1.isEmpty()) {
                first.setOriginCity(second.f1);
            } else {
                // use airport code as city code if city code unavailable
                first.setOriginCity(second.f0);
            }
            first.setOriginState(second.f2);
            first.setOriginCountry(second.f3);
            first.setOriginRegion(second.f4);
            first.setOriginLatitude(second.f5);
            first.setOriginLongitude(second.f6);

            if (!second.f1.isEmpty()) {
                first.setLastCity(second.f1);
            } else {
                // use airport code as city code if city code unavailable
                first.setLastCity(second.f0);
            }
            first.setLastState(second.f2);
            first.setLastCountry(second.f3);
            first.setLastRegion(second.f4);
            first.setLastLatitude(second.f5);
            first.setLastLongitude(second.f6);

            return first;
        }

    }

    @SuppressWarnings("serial")
    public static class DestinationCoordinateJoiner implements JoinFunction<Flight, Tuple7<String, String, String, String, String, Double, Double>, Flight> {

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

    public static class MultiLegJoiner implements FlatJoinFunction<Flight, Flight, Flight> {

        private final String exceptions = "AI";
        private final String exceptionsIn = "BG";

        @Override
        public void join(Flight in1, Flight in2, Collector<Flight> out) throws Exception {
            // sanity checks
            if (!in1.getDestinationCity().equals(in2.getOriginCity())) {
                throw new Exception("Hub city mismatch: " + in1.toString() + " / " + in2.toString());
            }
            if (!in1.getAirline().equals(in2.getAirline()) && in1.getFlightNumber().equals(in2.getFlightNumber())) {
                throw new Exception("Flight mismatch: " + in1.toString() + " / " + in2.toString());
            }
            if (exceptionsIn.indexOf(in1.getTrafficRestriction()) >= 0) {
                return;
            }
            if (((exceptions.indexOf(in1.getTrafficRestriction()) >= 0) && in2.getTrafficRestriction().equals(EMPTY)) ||
                    ((exceptions.indexOf(in2.getTrafficRestriction()) >= 0) && in1.getTrafficRestriction().equals(EMPTY))) {
                return;
            }
            long hubTime = in2.getDepartureTimestamp() - in1.getArrivalTimestamp();
            long travelTime = in2.getArrivalTimestamp() - in1.getDepartureTimestamp();
            if (hubTime <= 0L) {
                // arrival before departure
                return;
            }
            if (hubTime < (computeMinCT() * 60L * 1000L)) {
                return;
            }
            double ODDistance = dist(in1.getOriginLatitude(), in1.getOriginLongitude(), in2.getDestinationLatitude(), in2.getDestinationLongitude());
            if (hubTime > ((computeMaxCT(ODDistance)) * 60L * 1000L)) {
                return;
            }
            if (in1.getOriginAirport().equals(in2.getDestinationAirport())) {
                // some multi-leg flights are circular
                return;
            }
            Flight result = new Flight();

            result.setOriginAirport(in1.getOriginAirport());
            result.setOriginTerminal(in1.getOriginTerminal());
            result.setOriginCity(in1.getOriginCity());
            result.setOriginState(in1.getOriginState());
            result.setOriginCountry(in1.getOriginCountry());
            result.setOriginRegion(in1.getOriginRegion());
            result.setOriginLatitude(in1.getOriginLatitude());
            result.setOriginLongitude(in1.getOriginLongitude());
            result.setDepartureTimestamp(in1.getDepartureTimestamp());
            result.setDepartureWindow(in1.getDepartureWindow());

            result.setDestinationAirport(in2.getDestinationAirport());
            result.setDestinationTerminal(in2.getDestinationTerminal());
            result.setDestinationCity(in2.getDestinationCity());
            result.setDestinationState(in2.getDestinationState());
            result.setDestinationCountry(in2.getDestinationCountry());
            result.setDestinationRegion(in2.getDestinationRegion());
            result.setDestinationLatitude(in2.getDestinationLatitude());
            result.setDestinationLongitude(in2.getDestinationLongitude());
            result.setArrivalTimestamp(in2.getArrivalTimestamp());
            result.setFirstArrivalWindow(in2.getFirstArrivalWindow());
            result.setSecondArrivalWindow(in2.getSecondArrivalWindow());

            result.setAirline(in1.getAirline());
            result.setFlightNumber(in1.getFlightNumber());
            if (!in1.getAircraftType().equals(in2.getAircraftType())) {
                if (in1.getMaxCapacity() <= in2.getMaxCapacity()) {
                    result.setAircraftType(in1.getAircraftType());
                    result.setMaxCapacity(in1.getMaxCapacity());
                } else {
                    result.setAircraftType(in2.getAircraftType());
                    result.setMaxCapacity(in2.getMaxCapacity());
                }
            } else {
                result.setAircraftType(in1.getAircraftType());
                result.setMaxCapacity(in1.getMaxCapacity());
            }
            String codeshareInfo = "";
            if (!in1.getCodeShareInfo().isEmpty() && !in2.getCodeShareInfo().isEmpty()) {
                // merge codeshare info
                String[] codeshareInfo1 = in1.getCodeShareInfo().split("/");
                String[] codeshareInfo2 = in2.getCodeShareInfo().split("/");
                for (int i = 0; i < codeshareInfo1.length; i++) {
                    for (int j = 0; j < codeshareInfo2.length; j++) {
                        // keep all codeshare info that allows a connection over this multi-leg segment
                        if (codeshareInfo1[i].substring(0, 2).equals(codeshareInfo2[j].substring(0, 2))) {
                            codeshareInfo += codeshareInfo1[i] + "/";
                            codeshareInfo += codeshareInfo2[j] + "/";
                        }
                    }
                }
            }
            if (!codeshareInfo.isEmpty()) {
                codeshareInfo = codeshareInfo.substring(0, codeshareInfo.lastIndexOf('/'));
            }
            result.setCodeShareInfo(codeshareInfo);

            if (in1.getTrafficRestriction().equals('I') && in2.getTrafficRestriction().equals('I')) {
                result.setTrafficRestriction(EMPTY);
            } else if (in1.getTrafficRestriction().equals('A') && in2.getTrafficRestriction().equals('A')) {
                result.setTrafficRestriction(EMPTY);
            } else {
                result.setTrafficRestriction(in2.getTrafficRestriction());
            }

            result.setLastAirport(in2.getLastAirport());
            result.setLastTerminal(in2.getLastTerminal());
            result.setLastCity(in2.getLastCity());
            result.setLastState(in2.getLastState());
            result.setLastCountry(in2.getLastCountry());
            result.setLastRegion(in2.getLastRegion());
            result.setLastLatitude(in2.getLastLatitude());
            result.setLastLongitude(in2.getLastLongitude());

            result.setLegCount(in1.getLegCount() + in2.getLegCount());

            out.collect(result);
        }
    }

    @SuppressWarnings("serial")
    public static class FilteringConnectionJoiner implements FlatJoinFunction<Flight, Flight, Tuple2<Flight, Flight>> {

        private final String exceptionsGeneral = "ABHIMTDEG";
        private final String exceptionsInternational = "NOQW";
        private final String exceptionsDomestic = "C";

        @Override
        public void join(Flight in1, Flight in2, Collector<Tuple2<Flight, Flight>> out)
                throws Exception {
            // sanity check
            if (!in1.getDestinationCity().equals(in2.getOriginCity())) {
                throw new Exception("Hub city mismatch: " + in1.toString() + " / " + in2.toString());
            }
            long hubTime = in2.getDepartureTimestamp() - in1.getArrivalTimestamp();
            long travelTime = in2.getArrivalTimestamp() - in1.getDepartureTimestamp();
            if (hubTime <= 0L) {
                // arrival before departure
                return;
            }
            if (hubTime < (computeMinCT() * 60L * 1000L)) {
                return;
            }
            double ODDistance = dist(in1.getOriginLatitude(), in1.getOriginLongitude(), in2.getDestinationLatitude(), in2.getDestinationLongitude());
            if (hubTime > ((computeMaxCT(ODDistance)) * 60L * 1000L)) {
                return;
            }
            if (in1.getOriginAirport().equals(in2.getDestinationAirport())) {
                // some multi-leg flights are circular
                return;
            }
            if (in1.getAirline().equals(in2.getAirline()) && in1.getFlightNumber().equals(in2.getFlightNumber())) {
                // multi-leg flight connections have already been built
                return;
            }
            // check traffic restrictions
            if ((exceptionsGeneral.indexOf(in1.getTrafficRestriction()) >= 0) ||
                    (exceptionsGeneral.indexOf(in2.getTrafficRestriction()) >= 0)) {
                return;
            } else if (!isDomestic(in2) && exceptionsDomestic.indexOf(in1.getTrafficRestriction()) >= 0) {
                return;
            } else if (!isDomestic(in1) && exceptionsDomestic.indexOf(in2.getTrafficRestriction()) >= 0) {
                return;
            } else if (isDomestic(in2) && exceptionsInternational.indexOf(in1.getTrafficRestriction()) >= 0) {
                return;
            } else if (isDomestic(in1) && exceptionsInternational.indexOf(in2.getTrafficRestriction()) >= 0) {
                return;
            }
            if (isODDomestic(in1, in2)) {
                if (!isDomestic(in1)) {
                    if (hubTime > 240L * 60L * 1000L ||
                            !geoDetourAcceptable(in1.getOriginLatitude(), in1.getOriginLongitude(),
                                    in1.getDestinationLatitude(), in1.getDestinationLongitude(),
                                    in2.getDestinationLatitude(), in2.getDestinationLongitude())) {
                        // for domestic connections with the hub in another country the MaxCT is 240 min
                        // and geoDetour applies
                        return;
                    }
                } else {
                    if (travelTime > (travelTimeAt100kphInMinutes(ODDistance) * 60L * 1000L)) {
                        // if on a domestic flight we are faster than 100 km/h in a straight line between
                        // origin and destination the connection is built even if geoDetour is exceeded
                        // this is to preserve connection via domestic hubs like TLS-ORY-NCE
                        return;
                    }
                }
            } else {
                if (!geoDetourAcceptable(in1.getOriginLatitude(), in1.getOriginLongitude(),
                        in1.getDestinationLatitude(), in1.getDestinationLongitude(),
                        in2.getDestinationLatitude(), in2.getDestinationLongitude())) {
                    return;
                }
            }
            if (in1.getAirline().equals(in2.getAirline())) {
                // same airline
                out.collect(new Tuple2<Flight, Flight>(in1, in2));
            } else if (in1.getCodeShareInfo().isEmpty() && in2.getCodeShareInfo().isEmpty()) {
                // not the same airline and no codeshare information
                return;
            } else {
                // check if a connection can be made via codeshares
                String[] codeshareAirlines1 = null;
                if (!in1.getCodeShareInfo().isEmpty()) {
                    String[] codeshareInfo1 = in1.getCodeShareInfo().split("/");
                    codeshareAirlines1 = new String[codeshareInfo1.length + 1];
                    for (int i = 0; i < codeshareInfo1.length; i++) {
                        codeshareAirlines1[i] = codeshareInfo1[i].substring(0, 2);
                    }
                    codeshareAirlines1[codeshareAirlines1.length - 1] = in1.getAirline();
                } else {
                    codeshareAirlines1 = new String[]{in1.getAirline()};
                }
                String[] codeshareAirlines2 = null;
                if (!in2.getCodeShareInfo().isEmpty()) {
                    String[] codeshareInfo2 = in2.getCodeShareInfo().split("/");
                    codeshareAirlines2 = new String[codeshareInfo2.length + 1];
                    for (int i = 0; i < codeshareInfo2.length; i++) {
                        codeshareAirlines2[i] = codeshareInfo2[i].substring(0, 2);
                    }
                    codeshareAirlines2[codeshareAirlines2.length - 1] = in2.getAirline();
                } else {
                    codeshareAirlines2 = new String[]{in2.getAirline()};
                }
                for (int i = 0; i < codeshareAirlines1.length; i++) {
                    for (int j = 0; j < codeshareAirlines2.length; j++) {
                        if (codeshareAirlines1[i].equals(codeshareAirlines2[j])) {
                            out.collect(new Tuple2<Flight, Flight>(in1, in2));
                            return;
                        }
                    }
                }
            }
        }

    }

    public static class ThreeLegJoiner implements FlatJoinFunction<Tuple2<Flight, Flight>, Tuple2<Flight, Flight>, Tuple3<Flight, Flight, Flight>> {

        @Override
        public void join(Tuple2<Flight, Flight> in1, Tuple2<Flight, Flight> in2, Collector<Tuple3<Flight, Flight, Flight>> out) throws Exception {

            long hub1Time = in1.f1.getDepartureTimestamp() - in1.f0.getArrivalTimestamp();
            long hub2Time = in2.f1.getDepartureTimestamp() - in2.f0.getArrivalTimestamp();
            long hubTime = hub1Time + hub2Time;
            double ODDistance = dist(in1.f0.getOriginLatitude(), in1.f0.getOriginLongitude(),
                    in2.f1.getDestinationLatitude(), in2.f1.getDestinationLongitude());
            if (ODDistance < 1000.0) {
                return;
            }
            if (hubTime > (computeMaxCT(ODDistance) * 60L * 1000L)) {
                return;
            }
            if (in1.f0.getOriginAirport().equals(in2.f1.getDestinationAirport())) {
                return;
            }
            if (!geoDetourAcceptable(in1.f0.getOriginLatitude(), in1.f0.getOriginLongitude(),
                    in1.f0.getDestinationLatitude(), in1.f0.getDestinationLongitude(),
                    in1.f1.getDestinationLatitude(), in1.f1.getDestinationLongitude()) ||
                    !geoDetourAcceptable(in2.f0.getOriginLatitude(), in2.f0.getOriginLongitude(),
                            in2.f0.getDestinationLatitude(), in2.f0.getDestinationLongitude(),
                            in2.f1.getDestinationLatitude(), in2.f1.getDestinationLongitude())) {
                // if a two-leg connection exceeds the geoDetour it is unreasonable to build a
                // three-leg connection from it even if the two-leg connection makes sense
                return;
            }
            if (in1.f0.getAirline().equals(in1.f1.getAirline()) &&
                    in1.f0.getAirline().equals(in2.f1.getAirline()) &&
                    in1.f0.getFlightNumber().equals(in1.f1.getFlightNumber()) &&
                    in1.f0.getFlightNumber().equals(in2.f1.getFlightNumber())) {
                // we already built all multi-leg flights
                return;
            }
            if (in1.f0.getOriginCountry().equals(in2.f1.getDestinationCountry()) &&
                    (!in1.f0.getDestinationCountry().equals(in1.f0.getOriginCountry()) || !in1.f1.getDestinationCountry().equals(in1.f0.getOriginCountry()))) {
                // domestic three leg connections may not use foreign hubs
                return;
            }
            if (!geoDetourAcceptable(in1.f0.getOriginLatitude(), in1.f0.getOriginLongitude(),
                    in1.f0.getDestinationLatitude(), in1.f0.getDestinationLongitude(),
                    in2.f0.getOriginLatitude(), in2.f0.getOriginLongitude(),
                    in2.f1.getDestinationLatitude(), in2.f1.getDestinationLongitude())) {
                return;
            }
            // check if the codeshares still work (only compare first and last flight, we already checked if it's valid for the two-leg connections)
            if (in1.f0.getAirline().equals(in2.f1.getAirline())) {
                // same airline
                out.collect(new Tuple3<Flight, Flight, Flight>(in1.f0, in1.f1, in2.f1));
                return;
            } else if (in1.f0.getCodeShareInfo().isEmpty() && in2.f1.getCodeShareInfo().isEmpty()) {
                // not the same airline and no codeshare information
                return;
            } else {
                String[] codeshareAirlines1 = null;
                if (!in1.f0.getCodeShareInfo().isEmpty()) {
                    String[] codeshareInfo1 = in1.f0.getCodeShareInfo().split("/");
                    codeshareAirlines1 = new String[codeshareInfo1.length + 1];
                    for (int i = 0; i < codeshareInfo1.length; i++) {
                        codeshareAirlines1[i] = codeshareInfo1[i].substring(0, 2);
                    }
                    codeshareAirlines1[codeshareAirlines1.length - 1] = in1.f0.getAirline();
                } else {
                    codeshareAirlines1 = new String[]{in1.f0.getAirline()};
                }
                String[] codeshareAirlines2 = null;
                if (!in2.f1.getCodeShareInfo().isEmpty()) {
                    String[] codeshareInfo2 = in2.f1.getCodeShareInfo().split("/");
                    codeshareAirlines2 = new String[codeshareInfo2.length + 1];
                    for (int i = 0; i < codeshareInfo2.length; i++) {
                        codeshareAirlines2[i] = codeshareInfo2[i].substring(0, 2);
                    }
                    codeshareAirlines2[codeshareAirlines2.length - 1] = in2.f1.getAirline();
                } else {
                    codeshareAirlines2 = new String[]{in2.f1.getAirline()};
                }
                for (int i = 0; i < codeshareAirlines1.length; i++) {
                    for (int j = 0; j < codeshareAirlines2.length; j++) {
                        if (codeshareAirlines1[i].equals(codeshareAirlines2[j])) {
                            out.collect(new Tuple3<Flight, Flight, Flight>(in1.f0, in1.f1, in2.f1));
                            return;
                        }
                    }
                }
            }
        }

    }

    public static class DefaultCapacityJoiner implements JoinFunction<Flight, Tuple2<String, Integer>, Flight> {

        @Override
        public Flight join(Flight first, Tuple2<String, Integer> second) throws Exception {
            first.setMaxCapacity(second.f1);
            return first;
        }
    }

    public static class CapacityJoiner implements JoinFunction<Flight, Tuple3<String, String, Integer>, Flight> {

        @Override
        public Flight join(Flight first, Tuple3<String, String, Integer> second) throws Exception {
            first.setMaxCapacity(second.f2);
            return first;
        }
    }

    public static class MCTFilter extends RichCoGroupFunction<Tuple2<Flight, Flight>, MCTEntry, Tuple2<Flight, Flight>> {

        ArrayList<MCTEntry> DDList;
        ArrayList<MCTEntry> DIList;
        ArrayList<MCTEntry> IDList;
        ArrayList<MCTEntry> IIList;

        ArrayList<MCTEntry> DDWithAPChangeList;
        ArrayList<MCTEntry> DIWithAPChangeList;
        ArrayList<MCTEntry> IDWithAPChangeList;
        ArrayList<MCTEntry> IIWithAPChangeList;

        long defaultDD = 20L;
        long defaultDI = 60L;
        long defaultID = 60L;
        long defaultII = 60L;

        long defaultDDWithAPChange = 240L;
        long defaultDIWithAPChange = 240L;
        long defaultIDWithAPChange = 240L;
        long defaultIIWithAPChange = 240L;

        /*
        private long start = 0L;

        private ArrayList<Long> ruleLoadingTimes = new ArrayList<Long>(64);
        private ArrayList<Long> ruleCheckingTimes = new ArrayList<Long>(64);


        @Override
        public void open(Configuration parameters) {
            start = System.currentTimeMillis();
            LOG.info("CoGroupOperator opened at {} milliseconds.", start);
        }

        @Override
        public void close() {
            long end = System.currentTimeMillis();
            LOG.info("CoGroupOperator closed at {} milliseconds. Running Time: {}", end, end-start);
            long MAX = Long.MIN_VALUE;
            long MIN = Long.MAX_VALUE;
            long SUM = 0L;
            for(Long l : ruleLoadingTimes) {
                if(l.longValue() < MIN) {
                    MIN = l.longValue();
                }
                if(l.longValue() > MAX) {
                    MAX = l.longValue();
                }
                SUM += l.longValue();
            }
            LOG.info("Rule loading MIN: {} MAX: {} AVG: {}.", MIN, MAX, SUM/ruleLoadingTimes.size());
            for(Long l : ruleCheckingTimes) {
                if(l.longValue() < MIN) {
                    MIN = l.longValue();
                }
                if(l.longValue() > MAX) {
                    MAX = l.longValue();
                }
                SUM += l.longValue();
            }
            LOG.info("Rule checking MIN: {} MAX: {} AVG: {}.", MIN, MAX, SUM/ruleCheckingTimes.size());
        }*/

        @Override
        public void coGroup(Iterable<Tuple2<Flight, Flight>> connections, Iterable<MCTEntry> mctEntries, Collector<Tuple2<Flight, Flight>> out) throws Exception {
            long start = 0L;
            long end = 0L;
            //if(LOG.isInfoEnabled()) {
            //    start = System.currentTimeMillis();
            //}
            DDList = new ArrayList<MCTEntry>(1);
            DIList = new ArrayList<MCTEntry>(1);
            IDList = new ArrayList<MCTEntry>(1);
            IIList = new ArrayList<MCTEntry>(1);
            DDWithAPChangeList = new ArrayList<MCTEntry>(1);
            DIWithAPChangeList = new ArrayList<MCTEntry>(1);
            IDWithAPChangeList = new ArrayList<MCTEntry>(1);
            IIWithAPChangeList = new ArrayList<MCTEntry>(1);
            String stat = null;
            String dep = null;
            String arr = null;
            for (MCTEntry entry : mctEntries) {
                stat = entry.getStat();
                arr = entry.getArrival();
                dep = entry.getDeparture();
                if (dep.isEmpty()) {
                    if (stat.equals("DD")) {
                        DDList.add(entry);
                    } else if (stat.equals("DI")) {
                        DIList.add(entry);
                    } else if (stat.equals("ID")) {
                        IDList.add(entry);
                    } else if (stat.equals("II")) {
                        IIList.add(entry);
                    } else {
                        throw new Exception("Unknown stat: " + entry.toString());
                    }
                } else if (!arr.equals(dep)) {
                    if (stat.equals("DD")) {
                        DDWithAPChangeList.add(entry);
                    } else if (stat.equals("DI")) {
                        DIWithAPChangeList.add(entry);
                    } else if (stat.equals("ID")) {
                        IDWithAPChangeList.add(entry);
                    } else if (stat.equals("II")) {
                        IIWithAPChangeList.add(entry);
                    } else {
                        throw new Exception("Unknown stat: " + entry.toString());
                    }
                }
            }
            Collections.sort(DDList, new ScoreComparator());
            Collections.sort(DIList, new ScoreComparator());
            Collections.sort(IDList, new ScoreComparator());
            Collections.sort(IIList, new ScoreComparator());
            Collections.sort(DDWithAPChangeList, new ScoreComparator());
            Collections.sort(DIWithAPChangeList, new ScoreComparator());
            Collections.sort(IDWithAPChangeList, new ScoreComparator());
            Collections.sort(IIWithAPChangeList, new ScoreComparator());
            //if(LOG.isInfoEnabled()) {
            //   end = System.currentTimeMillis();
            //   ruleLoadingTimes.add(end-start);
            //   start = System.currentTimeMillis();
            //}
            ArrayList<MCTEntry> connStatList = null;
            long defaultMCT = 0L;
            for (Tuple2<Flight, Flight> conn : connections) {
                if (conn.f0.getDestinationAirport().equals(conn.f1.getOriginAirport())) {
                    if (conn.f0.getLastCountry().equals(conn.f0.getDestinationCountry())) {
                        if (conn.f1.getOriginCountry().equals(conn.f1.getDestinationCountry())) {
                            connStatList = DDList;
                            defaultMCT = defaultDD;
                        } else {
                            connStatList = DIList;
                            defaultMCT = defaultDI;
                        }
                    } else {
                        if (conn.f1.getLastCountry().equals(conn.f1.getDestinationCountry())) {
                            connStatList = IDList;
                            defaultMCT = defaultID;
                        } else {
                            connStatList = IIList;
                            defaultMCT = defaultII;
                        }
                    }
                } else {
                    if (conn.f0.getLastCountry().equals(conn.f0.getDestinationCountry())) {
                        if (conn.f1.getOriginCountry().equals(conn.f1.getDestinationCountry())) {
                            connStatList = DDWithAPChangeList;
                            defaultMCT = defaultDDWithAPChange;
                        } else {
                            connStatList = DIWithAPChangeList;
                            defaultMCT = defaultDIWithAPChange;
                        }
                    } else {
                        if (conn.f1.getLastCountry().equals(conn.f1.getDestinationCountry())) {
                            connStatList = IDWithAPChangeList;
                            defaultMCT = defaultIDWithAPChange;
                        } else {
                            connStatList = IIWithAPChangeList;
                            defaultMCT = defaultIIWithAPChange;
                        }
                    }
                }
                long MCT = 0L;
                int bestResult = 0;
                // search for best rule in connStatList
                int tmpResult = 0;
                MCTEntry bestRule = null;
                for (MCTEntry e : connStatList) {
                    if (match(conn, e)) {
                        bestRule = e;
                        break; // first hit is best rule since the list is sorted by score
                    }
                }
                if (bestRule != null) {
                    MCT = bestRule.getMCT();
                } else {
                    MCT = defaultMCT;
                }
                if (checkWithMCT(conn, MCT)) {
                    out.collect(conn);
                }
            }
            //if(LOG.isInfoEnabled()) {
            //    end = System.currentTimeMillis();
            //    ruleCheckingTimes.add(end-start);
            //}
        }

        private static boolean checkWithMCT(Tuple2<Flight, Flight> conn, long MCT) {
            if (MCT >= 999L) {
                return false;
            }
            long MinCT = MCT * 60L * 1000L;
            long hubTime = conn.f1.getDepartureTimestamp() - conn.f0.getArrivalTimestamp();
            if (hubTime < MinCT) {
                return false;
            } else {
                return true;
            }
        }

        public static class ScoreComparator implements Comparator<MCTEntry> {

            @Override
            public int compare(MCTEntry mctEntry, MCTEntry mctEntry2) {
                // descending order
                return scoreRule(mctEntry2) - scoreRule(mctEntry);
            }

            @Override
            public boolean equals(Object o) {
                // MCT rules are assumed to be unique
                return false;
            }
        }

        public static int scoreRule(MCTEntry rule) {
            int result = 0;
            if (rule.getStat().isEmpty())
                return 0;
            if (!rule.getDeparture().isEmpty())
                result += 1 << 0;
            if (!rule.getArrival().isEmpty())
                result += 1 << 1;
            if (!rule.getDepTerminal().isEmpty())
                result += 1 << 2;
            if (!rule.getArrTerminal().isEmpty())
                result += 1 << 3;
            if (!rule.getNextRegion().isEmpty())
                result += 1 << 4;
            if (!rule.getPrevRegion().isEmpty())
                result += 1 << 5;
            if (!rule.getNextCountry().isEmpty())
                result += 1 << 6;
            if (!rule.getPrevCountry().isEmpty())
                result += 1 << 7;
            if (!rule.getNextState().isEmpty())
                result += 1 << 8;
            if (!rule.getPrevState().isEmpty())
                result += 1 << 9;
            if (!rule.getNextCity().isEmpty())
                result += 1 << 10;
            if (!rule.getPrevCity().isEmpty())
                result += 1 << 11;
            if (!rule.getNextAirport().isEmpty())
                result += 1 << 12;
            if (!rule.getPrevAirport().isEmpty())
                result += 1 << 13;
            if (!rule.getDepAircraft().isEmpty())
                result += 1 << 14;
            if (!rule.getArrAircraft().isEmpty())
                result += 1 << 15;
            if (!rule.getDepCarrier().isEmpty()) {
                result += 1 << 16;
                if (rule.getOutFlightNumber().intValue() != 0) {
                    result += 1 << 18;
                }
            }
            if (!rule.getArrCarrier().isEmpty()) {
                result += 1 << 17;
                if (rule.getInFlightNumber().intValue() != 0) {
                    result += 1 << 19;
                }
            }
            return result;
        }

        private static boolean match(Tuple2<Flight, Flight> conn, MCTEntry rule) {
            if (!rule.getArrival().isEmpty()) {
                if (!conn.f0.getDestinationAirport().equals(rule.getArrival())) {
                    return false;
                }
            }
            if (!rule.getDeparture().isEmpty()) {
                if (!conn.f1.getLastAirport().equals(rule.getDeparture())) {
                    return false;
                }
            }
            if (!rule.getArrCarrier().isEmpty()) {
                if (!conn.f0.getAirline().equals(rule.getArrCarrier())) {
                    return false;
                } else {
                    if (rule.getInFlightNumber().intValue() != 0) {
                        if (!(conn.f0.getFlightNumber() >= rule.getInFlightNumber() &&
                                conn.f0.getFlightNumber() <= rule.getInFlightEOR())) {
                            return false;
                        }
                    }
                }
            }
            if (!rule.getDepCarrier().isEmpty()) {
                if (!conn.f1.getAirline().equals(rule.getDepCarrier())) {
                    return false;
                } else {
                    if (rule.getOutFlightNumber().intValue() != 0) {
                        if (!(conn.f1.getFlightNumber() >= rule.getOutFlightNumber() &&
                                conn.f1.getFlightNumber() <= rule.getOutFlightEOR())) {
                            return false;
                        }
                    }
                }
            }
            if (!rule.getArrAircraft().isEmpty()) {
                if (!conn.f0.getAircraftType().equals(rule.getArrAircraft())) {
                    return false;
                }
            }
            if (!rule.getDepAircraft().isEmpty()) {
                if (!conn.f1.getAircraftType().equals(rule.getDepAircraft())) {
                    return false;
                }
            }
            if (!rule.getArrTerminal().isEmpty()) {
                if (!conn.f0.getDestinationTerminal().equals(rule.getArrTerminal())) {
                    return false;
                }
            }
            if (!rule.getDepTerminal().isEmpty()) {
                if (!conn.f1.getLastTerminal().equals(rule.getDepTerminal())) {
                    return false;
                }
            }
            if (!rule.getPrevCountry().isEmpty()) {
                if (!conn.f0.getLastCountry().equals(rule.getPrevCountry())) {
                    return false;
                }
            }
            if (!rule.getPrevCity().isEmpty()) {
                if (!conn.f0.getLastCity().equals(rule.getPrevCity())) {
                    return false;
                }
            }
            if (!rule.getPrevAirport().isEmpty()) {
                if (!conn.f0.getLastAirport().equals(rule.getPrevAirport())) {
                    return false;
                }
            }
            if (!rule.getNextCountry().isEmpty()) {
                if (!conn.f1.getDestinationCountry().equals(rule.getNextCountry())) {
                    return false;
                }
            }
            if (!rule.getNextCity().isEmpty()) {
                if (!conn.f1.getDestinationCity().equals(rule.getNextCity())) {
                    return false;
                }
            }
            if (!rule.getNextAirport().isEmpty()) {
                if (!conn.f1.getDestinationAirport().equals(rule.getNextAirport())) {
                    return false;
                }
            }
            if (!rule.getPrevState().isEmpty()) {
                if (!conn.f0.getLastState().equals(rule.getPrevState())) {
                    return false;
                }
            }
            if (!rule.getNextState().isEmpty()) {
                if (!conn.f1.getDestinationState().equals(rule.getNextState())) {
                    return false;
                }
            }
            if (!rule.getPrevRegion().isEmpty()) {
                // SCH is a real subset of EUR
                if (!rule.getPrevRegion().equals("EUR") || !rule.getPrevRegion().equals("SCH")) {
                    if (!conn.f0.getLastRegion().equals(rule.getPrevRegion())) {
                        return false;
                    }
                } else {
                    if (!conn.f0.getLastRegion().equals("SCH") || !conn.f0.getLastRegion().equals("EUR")) {
                        return false;
                    }
                }
            }
            if (!rule.getNextRegion().isEmpty()) {
                // SCH is a real subset of EUR
                if (!rule.getNextRegion().equals("EUR") || !rule.getNextRegion().equals("SCH")) {
                    if (!conn.f1.getDestinationRegion().equals(rule.getNextRegion())) {
                        return false;
                    }
                } else {
                    if (!conn.f1.getDestinationRegion().equals("SCH") || !conn.f1.getDestinationRegion().equals("EUR")) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    /**
     * Discard all flights that depart outside the given date range
     */
    public static class DateRangeFilter implements FilterFunction<Flight> {

        @Override
        public boolean filter(Flight flight) throws Exception {
            if (flight.getDepartureTimestamp() < START || flight.getDepartureTimestamp() > END) {
                return false;
            } else {
                return true;
            }
        }
    }

    /**
     * Discard all non-stop flights that can only be part of multileg flights
     */
    public static class NonStopTrafficRestrictionsFilter implements FilterFunction<Flight> {

        private final String exceptions = "AIKNOY";

        @Override
        public boolean filter(Flight value) throws Exception {
            if (exceptions.indexOf(value.getTrafficRestriction()) >= 0) {
                return false;
            } else {
                return true;
            }
        }
    }

    public static class TwoLegTrafficRestrictionsFilter implements FilterFunction<Tuple2<Flight, Flight>> {

        private final String exceptionsGeneral = "ABHIMTDEG";
        private final String exceptionsInternational = "NOQW";
        private final String exceptionsDomestic = "C";

        @Override
        public boolean filter(Tuple2<Flight, Flight> value) throws Exception {
            if (exceptionsGeneral.indexOf(value.f0.getTrafficRestriction()) >= 0) {
                return false;
            } else if (!isDomestic(value.f1) && exceptionsDomestic.indexOf(value.f0.getTrafficRestriction()) >= 0) {
                return false;
            } else if (!isDomestic(value.f0) && exceptionsDomestic.indexOf(value.f1.getTrafficRestriction()) >= 0) {
                return false;
            } else if (isDomestic(value.f1) && exceptionsInternational.indexOf(value.f0.getTrafficRestriction()) >= 0) {
                return false;
            } else if (isDomestic(value.f0) && exceptionsInternational.indexOf(value.f1.getTrafficRestriction()) >= 0) {
                return false;
            } else {
                return true;
            }
        }
    }

    public static class ThreeLegTrafficRestrictionsFilter implements FilterFunction<Tuple3<Flight, Flight, Flight>> {

        private final String exceptionsGeneral = "ABHIMTDEG";
        private final String exceptionsInternational = "NOQW";
        private final String exceptionsDomestic = "C";

        @Override
        public boolean filter(Tuple3<Flight, Flight, Flight> value) throws Exception {
            return true;
        }
    }

    public static class NonStopInternationalFilter implements FilterFunction<Flight> {

        @Override
        public boolean filter(Flight value) throws Exception {
            if (value.getDepartureTimestamp() < WEEK_START || value.getDepartureTimestamp() > WEEK_END) {
                return false;
            } else {
                return !value.getOriginCountry().equals(value.getDestinationCountry());
            }
        }
    }

    public static class TwoLegInternationalFilter implements FilterFunction<Tuple2<Flight, Flight>> {

        @Override
        public boolean filter(Tuple2<Flight, Flight> value) throws Exception {
            if (value.f0.getDepartureTimestamp() < WEEK_START || value.f0.getDepartureTimestamp() > WEEK_END &&
                    value.f1.getDepartureTimestamp() < WEEK_START || value.f1.getDepartureTimestamp() > WEEK_END) {
                return false;
            } else {
                return !(value.f0.getOriginCountry().equals(value.f0.getDestinationCountry()) ||
                        value.f1.getOriginCountry().equals(value.f1.getDestinationCountry()));
            }
        }
    }

    public static class ThreeLegInternationalFilter implements FilterFunction<Tuple3<Flight, Flight, Flight>> {

        @Override
        public boolean filter(Tuple3<Flight, Flight, Flight> value) throws Exception {
            if (value.f0.getDepartureTimestamp() < WEEK_START || value.f0.getDepartureTimestamp() > WEEK_END &&
                    value.f2.getDepartureTimestamp() < WEEK_START || value.f2.getDepartureTimestamp() > WEEK_END) {
                return false;
            } else {
                return !(value.f0.getOriginCountry().equals(value.f0.getDestinationCountry()) ||
                        value.f1.getOriginCountry().equals(value.f1.getDestinationCountry()) ||
                        value.f2.getOriginCountry().equals(value.f2.getDestinationCountry()));
            }
        }
    }

    public static class CapacityExtractor1 implements GroupReduceFunction<Flight, ODCapacity> {

        @Override
        public void reduce(Iterable<Flight> flights, Collector<ODCapacity> out) throws Exception {
            ODCapacity result = new ODCapacity();
            boolean first = true;
            int capacity = 0;
            for (Flight flight : flights) {
                if (first) {
                    result.f0 = flight.getOriginCity();
                    result.f1 = flight.getDestinationCity();
                    first = false;
                }
                capacity += flight.getMaxCapacity();
            }
            result.f2 = capacity;
            out.collect(result);
        }
    }

    public static class CapacityExtractor2 implements GroupReduceFunction<Tuple2<Flight, Flight>, ODCapacity> {

        @Override
        public void reduce(Iterable<Tuple2<Flight, Flight>> connections, Collector<ODCapacity> out) throws Exception {
            ODCapacity result = new ODCapacity();
            boolean first = true;
            int capacity = 0;
            for (Tuple2<Flight, Flight> connection : connections) {
                if (first) {
                    result.f0 = connection.f0.getOriginCity();
                    result.f1 = connection.f1.getDestinationCity();
                    first = false;
                }
                capacity += getMinCap(connection);
            }
            result.f2 = capacity;
            out.collect(result);
        }
    }

    public static class CapacityExtractor3 implements GroupReduceFunction<Tuple3<Flight, Flight, Flight>, ODCapacity> {

        @Override
        public void reduce(Iterable<Tuple3<Flight, Flight, Flight>> connections, Collector<ODCapacity> out) throws Exception {
            ODCapacity result = new ODCapacity();
            boolean first = true;
            int capacity = 0;
            for (Tuple3<Flight, Flight, Flight> connection : connections) {
                if (first) {
                    result.f0 = connection.f0.getOriginCity();
                    result.f1 = connection.f2.getDestinationCity();
                    first = false;
                }
                capacity += getMinCap(connection);
            }
            result.f2 = capacity;
            out.collect(result);
        }
    }

    public static class CapacityReducer1 implements GroupReduceFunction<Flight, ItineraryInfo.ItineraryInfo1> {

        @Override
        public void reduce(Iterable<Flight> flights, Collector<ItineraryInfo.ItineraryInfo1> out) throws Exception {
            ItineraryInfo.ItineraryInfo1 result = new ItineraryInfo.ItineraryInfo1();
            int travelTime = 0;
            int minTime = Integer.MAX_VALUE;
            int maxTime = Integer.MIN_VALUE;
            int sumTime = 0;
            int count = 0;
            boolean first = true;
            int capacity = 0;
            for (Flight flight : flights) {
                if (first) {
                    result.f0 = flight.f0;
                    result.f1 = flight.f2;
                    result.f2 = flight.getAirline();
                    first = false;
                }
                travelTime = (int) ((flight.getArrivalTimestamp() - flight.getDepartureTimestamp()) / (60L * 1000L));
                sumTime += travelTime;
                count++;
                if (travelTime < minTime)
                    minTime = travelTime;
                if (travelTime > maxTime)
                    maxTime = travelTime;
                capacity += flight.getMaxCapacity();
            }
            result.f3 = capacity;
            result.f5 = minTime;
            result.f6 = maxTime;
            result.f7 = sumTime / count;
            out.collect(result);
        }
    }

    public static class CapacityReducer2 implements GroupReduceFunction<Tuple2<Flight, Flight>, ItineraryInfo.ItineraryInfo2> {

        @Override
        public void reduce(Iterable<Tuple2<Flight, Flight>> connections, Collector<ItineraryInfo.ItineraryInfo2> out) throws Exception {
            ItineraryInfo.ItineraryInfo2 result = new ItineraryInfo.ItineraryInfo2();
            int travelTime = 0;
            int minTime = Integer.MAX_VALUE;
            int maxTime = Integer.MIN_VALUE;
            int sumTime = 0;
            int count = 0;
            boolean first = true;
            int capacity = 0;
            for (Tuple2<Flight, Flight> connection : connections) {
                if (first) {
                    result.f0 = connection.f0.f0;
                    result.f1 = connection.f0.f2;
                    result.f2 = connection.f0.getAirline();
                    result.f3 = connection.f1.f0;
                    result.f4 = connection.f1.f2;
                    result.f5 = connection.f1.getAirline();
                    first = false;
                }
                travelTime = (int) ((connection.f1.getArrivalTimestamp() - connection.f0.getDepartureTimestamp()) / (60L * 1000L));
                sumTime += travelTime;
                count++;
                if (travelTime < minTime)
                    minTime = travelTime;
                if (travelTime > maxTime)
                    maxTime = travelTime;
                capacity += getMinCap(connection);
            }
            result.f6 = capacity;
            result.f8 = minTime;
            result.f9 = maxTime;
            result.f10 = sumTime / count;
            out.collect(result);
        }
    }

    public static class CapacityReducer3 implements GroupReduceFunction<Tuple3<Flight, Flight, Flight>, ItineraryInfo.ItineraryInfo3> {

        @Override
        public void reduce(Iterable<Tuple3<Flight, Flight, Flight>> connections, Collector<ItineraryInfo.ItineraryInfo3> out) throws Exception {
            ItineraryInfo.ItineraryInfo3 result = new ItineraryInfo.ItineraryInfo3();
            int travelTime = 0;
            int minTime = Integer.MAX_VALUE;
            int maxTime = Integer.MIN_VALUE;
            int sumTime = 0;
            int count = 0;
            boolean first = true;
            int capacity = 0;
            for (Tuple3<Flight, Flight, Flight> connection : connections) {
                if (first) {
                    result.f0 = connection.f0.f0;
                    result.f1 = connection.f0.f2;
                    result.f2 = connection.f0.getAirline();
                    result.f3 = connection.f1.f0;
                    result.f4 = connection.f1.f2;
                    result.f5 = connection.f1.getAirline();
                    result.f6 = connection.f2.f0;
                    result.f7 = connection.f2.f2;
                    result.f8 = connection.f2.getAirline();
                    first = false;
                }
                travelTime = (int) ((connection.f2.getArrivalTimestamp() - connection.f0.getDepartureTimestamp()) / (60L * 1000L));
                sumTime += travelTime;
                count++;
                if (travelTime < minTime)
                    minTime = travelTime;
                if (travelTime > maxTime)
                    maxTime = travelTime;
                capacity += getMinCap(connection);
            }
            result.f9 = capacity;
            result.f11 = minTime;
            result.f12 = maxTime;
            result.f13 = sumTime / count;
            out.collect(result);
        }

    }

    public static class CapShareJoiner1 implements JoinFunction<ItineraryInfo.ItineraryInfo1, ODCapacity, ItineraryInfo.ItineraryInfo1> {

        @Override
        public ItineraryInfo.ItineraryInfo1 join(ItineraryInfo.ItineraryInfo1 itineraryInfo1, ODCapacity odCapacity) throws Exception {
            int capacity = itineraryInfo1.f3;
            double share = (double) capacity / (double) odCapacity.f2;
            itineraryInfo1.f4 = share;
            return itineraryInfo1;
        }
    }

    public static class CapShareJoiner2 implements JoinFunction<ItineraryInfo.ItineraryInfo2, ODCapacity, ItineraryInfo.ItineraryInfo2> {

        @Override
        public ItineraryInfo.ItineraryInfo2 join(ItineraryInfo.ItineraryInfo2 itineraryInfo2, ODCapacity odCapacity) throws Exception {
            int capacity = itineraryInfo2.f6;
            double share = (double) capacity / (double) odCapacity.f2;
            itineraryInfo2.f7 = share;
            return itineraryInfo2;
        }
    }

    public static class CapShareJoiner3 implements JoinFunction<ItineraryInfo.ItineraryInfo3, ODCapacity, ItineraryInfo.ItineraryInfo3> {

        @Override
        public ItineraryInfo.ItineraryInfo3 join(ItineraryInfo.ItineraryInfo3 itineraryInfo3, ODCapacity odCapacity) throws Exception {
            int capacity = itineraryInfo3.f9;
            double share = (double) capacity / (double) odCapacity.f2;
            itineraryInfo3.f10 = share;
            return itineraryInfo3;
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        LOG.info("Job started at {} milliseconds.", start);
        int phase = 0;
        if (args.length == 1) {
            phase = Integer.parseInt(args[0]);
        }
        // the program is split into two parts (building and parsing all non-stop connections, and finding multi-leg flights)
        if (phase == 0) {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // get all relevant schedule data
            DataSet<String> flights = env.readTextFile(schedulePath);
            DataSet<Flight> extracted = flights.flatMap(new FilteringUTCExtractor());

            // extract GPS coordinates of all known airports
            DataSet<String> rawAirportInfo = env.readTextFile(oriPath);
            DataSet<Tuple7<String, String, String, String, String, Double, Double>> airportCoordinatesNR = rawAirportInfo.flatMap(new AirportCoordinateExtractor());

            // get IATA region information for each airport
            DataSet<String> rawRegionInfo = env.readTextFile(regionPath);
            DataSet<Tuple2<String, String>> regionInfo = rawRegionInfo.map(new RegionExtractor());

            DataSet<Tuple7<String, String, String, String, String, Double, Double>> airportCoordinates =
                    airportCoordinatesNR.join(regionInfo).where(3).equalTo(0).with(new RegionJoiner());

            /*
            KeySelector<Flight, String> jk1 = new KeySelector<Flight, String>() {
                public String getKey(Flight tuple) {
                    return tuple.getOriginAirport();
                }
            };
            KeySelector<Flight, String> jk2 = new KeySelector<Flight, String>() {
                public String getKey(Flight tuple) {
                    return tuple.getDestinationAirport();
                }
            };
            */

            DataSet<Flight> join1 = extracted.join(airportCoordinates, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
                    .where("f0.f0").equalTo(0).with(new OriginCoordinateJoiner());
            DataSet<Flight> join2 = join1.join(airportCoordinates, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
                    .where("f2.f0").equalTo(0).with(new DestinationCoordinateJoiner());

            // add aircraft capacities (first default then overwrite with airline specific information if available)
            DataSet<Tuple2<String, Integer>> defaultCapacities = env.readCsvFile(defaultCapacityPath).types(String.class, Integer.class);
            DataSet<Flight> join3 = join2.join(defaultCapacities).where("f6").equalTo(0).with(new DefaultCapacityJoiner());

            DataSet<Tuple3<String, String, Integer>> aircraftCapacities = env.readCsvFile(capacityPath).fieldDelimiter('^').ignoreFirstLine()
                    .includeFields(false, true, true, true, false, false, false, false).types(String.class, String.class, Integer.class);

            DataSet<Flight> join4 = join3.join(aircraftCapacities).where("f6", "f4").equalTo(0, 1).with(new CapacityJoiner());

            /*
            KeySelector<Flight, Tuple3<String, String, Integer>> ml1 = new KeySelector<Flight, Tuple3<String, String, Integer>>() {
                public Tuple3<String, String, Integer> getKey(Flight tuple) {
                    return new Tuple3<String, String, Integer>(tuple.getOriginCity(), tuple.getAirline(), tuple.getFlightNumber());
                }
            };
            KeySelector<Flight, Tuple3<String, String, Integer>> ml2 = new KeySelector<Flight, Tuple3<String, String, Integer>>() {
                 public Tuple3<String, String, Integer> getKey(Flight tuple) {
                     return new Tuple3<String, String, Integer>(tuple.getDestinationCity(), tuple.getAirline(), tuple.getFlightNumber());
                 }
            };
            */

            // create multi-leg flights as non-stop flights
            DataSet<Flight> multiLeg2a = join4.join(join4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg2b = join4.join(join4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg2 = multiLeg2a.union(multiLeg2b);

            DataSet<Flight> multiLeg3a = multiLeg2.join(join4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg3b = multiLeg2.join(join4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg3 = multiLeg3a.union(multiLeg3b);

            DataSet<Flight> multiLeg4a = multiLeg2.join(multiLeg2, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg4b = multiLeg2.join(multiLeg2, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg4 = multiLeg4a.union(multiLeg4b);

            DataSet<Flight> multiLeg5a = multiLeg2.join(multiLeg3, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg5b = multiLeg2.join(multiLeg3, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg5 = multiLeg5a.union(multiLeg5b);

            DataSet<Flight> multiLeg6a = multiLeg3.join(multiLeg3, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg6b = multiLeg3.join(multiLeg3, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg6 = multiLeg6a.union(multiLeg6b);

            DataSet<Flight> multiLeg7a = multiLeg3.join(multiLeg4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg7b = multiLeg3.join(multiLeg4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg7 = multiLeg7a.union(multiLeg7b);

            DataSet<Flight> multiLeg8a = multiLeg4.join(multiLeg4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg8b = multiLeg4.join(multiLeg4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg8 = multiLeg8a.union(multiLeg8b);

            DataSet<Flight> singleFltNoFlights = join4.union(multiLeg2).union(multiLeg3).union(multiLeg4).union(multiLeg5).union(multiLeg6).union(multiLeg7).union(multiLeg8);

            /*
            FileOutputFormat multi2 = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath+"multileg2/"));
            multiLeg2.write(multi2, outputPath+"multileg2/", WriteMode.OVERWRITE);
            FileOutputFormat multi3 = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath+"multileg3/"));
            multiLeg3.write(multi3, outputPath+"multileg3/", WriteMode.OVERWRITE);
            FileOutputFormat multi4 = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath+"multileg4/"));
            multiLeg4.write(multi4, outputPath+"multileg4/", WriteMode.OVERWRITE);
            FileOutputFormat multi5 = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath+"multileg5/"));
            multiLeg5.write(multi5, outputPath+"multileg5/", WriteMode.OVERWRITE);
            FileOutputFormat multi6 = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath+"multileg6/"));
            multiLeg6.write(multi6, outputPath+"multileg6/", WriteMode.OVERWRITE);
            FileOutputFormat multi7 = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath+"multileg7/"));
            multiLeg7.write(multi7, outputPath+"multileg7/", WriteMode.OVERWRITE);
            FileOutputFormat multi8 = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath+"multileg8/"));
            multiLeg8.write(multi8, outputPath+"multileg8/", WriteMode.OVERWRITE);
            */

            FileOutputFormat nonStop = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath + "one"));
            singleFltNoFlights.filter(new NonStopTrafficRestrictionsFilter()).write(nonStop, outputPath + "one", WriteMode.OVERWRITE);

            FileOutputFormat nonStopFull = new FlightOutput.NonStopFullOutputFormat();
            singleFltNoFlights.write(nonStopFull, outputPath + "oneFull", WriteMode.OVERWRITE);


            env.execute("Phase 0");
            //System.out.println(env.getExecutionPlan());
        } else if (phase == 1) {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Flight> singleFltNoFlights2 = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull/");

            KeySelector<Flight, String> tl1 = new KeySelector<Flight, String>() {
                public String getKey(Flight tuple) {
                    return tuple.getOriginAirport();
                }
            };
            KeySelector<Flight, String> tl2 = new KeySelector<Flight, String>() {
                public String getKey(Flight tuple) {
                    return tuple.getDestinationAirport();
                }
            };
            DataSet<Tuple2<Flight, Flight>> twoLegConnections1 = singleFltNoFlights2.join(singleFltNoFlights2, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
                    .where("f2.f2", "f13").equalTo("f0.f2", "f12").with(new FilteringConnectionJoiner());
            DataSet<Tuple2<Flight, Flight>> twoLegConnections2 = singleFltNoFlights2.join(singleFltNoFlights2, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
                    .where("f2.f2", "f14").equalTo("f0.f2", "f12").with(new FilteringConnectionJoiner());

            DataSet<Tuple2<Flight, Flight>> twoLegConnections = twoLegConnections1.union(twoLegConnections2);


            //FileOutputFormat twoLegUF = new FlightOutput.TwoLegFlightOutputFormat(new Path(outputPath+"twoUF/"));
            //twoLegConnections.write(twoLegUF, outputPath+"twoUF/", WriteMode.OVERWRITE);

            DataSet<MCTEntry> mctData = env.readCsvFile(mctPath)
                    .includeFields(true, true, true, true, true, true,
                            true, true, true, true, true, true,
                            true, true, true, true, true, true,
                            true, true, true, true, true, false,
                            false, true).ignoreFirstLine().tupleType(MCTEntry.class);

            //mctData.writeAsText(outputPath+"mctOutput/");

            DataSet<Tuple2<Flight, Flight>> twoLegConnectionsFiltered = twoLegConnections.coGroup(mctData).where("f0.f2.f0").equalTo(0).with(new MCTFilter());

            FileOutputFormat twoLeg = new FlightOutput.TwoLegFlightOutputFormat(new Path(outputPath + "two"));
            twoLegConnectionsFiltered.write(twoLeg, outputPath + "two", WriteMode.OVERWRITE);

            FileOutputFormat twoLegFull = new FlightOutput.TwoLegFullOutputFormat();
            twoLegConnectionsFiltered.write(twoLegFull, outputPath + "twoFull", WriteMode.OVERWRITE);

            KeySelector<Tuple2<Flight, Flight>, String> ks0 = new KeySelector<Tuple2<Flight, Flight>, String>() {
                public String getKey(Tuple2<Flight, Flight> tuple) {
                    return tuple.f0.getKey();
                }
            };
            KeySelector<Tuple2<Flight, Flight>, String> ks1 = new KeySelector<Tuple2<Flight, Flight>, String>() {
                public String getKey(Tuple2<Flight, Flight> tuple) {
                    return tuple.f1.getKey();
                }
            };

            DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections
                    = twoLegConnectionsFiltered.join(twoLegConnectionsFiltered, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
                    .where("f1.f0.f0", "f1.f1", "f1.f2.f0", "f1.f3", "f1.f4", "f1.f5").equalTo("f0.f0.f0", "f0.f1", "f0.f2.f0", "f0.f3", "f0.f4", "f0.f5").with(new ThreeLegJoiner());

            FileOutputFormat threeLeg = new FlightOutput.ThreeLegFlightOutputFormat(new Path(outputPath + "three"));
            threeLegConnections.write(threeLeg, outputPath + "three", WriteMode.OVERWRITE);

            FileOutputFormat threeLegFull = new FlightOutput.ThreeLegFullOutputFormat();
            threeLegConnections.write(threeLegFull, outputPath + "threeFull", WriteMode.OVERWRITE);

            //DataSet<ConnectionStats> ODCounts1 = join4.groupBy(0,9).reduceGroup(new ODCounter1());
            //ODCounts1.writeAsText(outputPath+"counts/one/", WriteMode.OVERWRITE);

            //DataSet<ConnectionStats> ODCounts2 = twoLegConnectionsFiltered.groupBy("f0.f0", "f1.f9").reduceGroup(new ODCounter2());
            //ODCounts2.writeAsText(outputPath+"counts/two/", WriteMode.OVERWRITE);

            //DataSet<ConnectionStats> ODCounts3 = threeLegConnections.groupBy("f0.f0", "f2.f9").reduceGroup(new ODCounter3());
            //ODCounts3.writeAsText(outputPath+"counts/three/", WriteMode.OVERWRITE);

            //DataSet<ConnectionStats> union = ODCounts1.union(ODCounts2).union(ODCounts3);
            //union.writeAsText(outputPath+"counts/union/", WriteMode.OVERWRITE);

            //union.groupBy(0,1).sum(2).andMax(3).andMin(4).andSum(5).andSum(6).andMin(7).writeAsText(outputPath+"counts/aggregated/", WriteMode.OVERWRITE);

            env.execute("Phase 1");
            //System.out.println(env.getExecutionPlan());
        } else if (phase == 2) {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull/");
            DataSet<Tuple2<Flight, Flight>> twoLegConnections = env.readFile(new FlightOutput.TwoLegFullInputFormat(), outputPath + "twoFull/");
            DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), outputPath + "threeFull/");


            DataSet<ODCapacity> nonStopCaps = nonStopConnections.filter(new NonStopInternationalFilter())
                    .groupBy("f0.f2", "f2.f2").reduceGroup(new CapacityExtractor1());
            DataSet<ODCapacity> twoLegCaps = twoLegConnections.filter(new TwoLegInternationalFilter())
                    .groupBy("f0.f0.f2", "f1.f2.f2").reduceGroup(new CapacityExtractor2());
            DataSet<ODCapacity> threeLegCaps = threeLegConnections.filter(new ThreeLegInternationalFilter())
                    .groupBy("f0.f0.f2", "f2.f2.f2").reduceGroup(new CapacityExtractor3());

            DataSet<ODCapacity> ODCaps = nonStopCaps.union(twoLegCaps).union(threeLegCaps).groupBy(0, 1).sum(2);

            ODCaps.writeAsCsv(outputPath + "ODCaps", "\n", "^", WriteMode.OVERWRITE);

            // group keys: origin, destination, airline
            DataSet<ItineraryInfo.ItineraryInfo1> ii1 = nonStopConnections.filter(new NonStopInternationalFilter())
                    .groupBy("f0.f0", "f2.f0", "f4").reduceGroup(new CapacityReducer1())
                    .join(ODCaps, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f0.f2", "f1.f2").equalTo(0, 1).with(new CapShareJoiner1());
            // group keys: origin, destination, airline, origin, destination, airline
            DataSet<ItineraryInfo.ItineraryInfo2> ii2 = twoLegConnections.filter(new TwoLegInternationalFilter())
                    .groupBy("f0.f0.f0", "f0.f2.f0", "f0.f4", "f1.f0.f0", "f1.f2.f0", "f1.f4").reduceGroup(new CapacityReducer2())
                    .join(ODCaps, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f0.f2", "f4.f2").equalTo(0, 1).with(new CapShareJoiner2());
            // group keys: origin, destination, airline, origin, destination, airline, origin, destination, airline
            DataSet<ItineraryInfo.ItineraryInfo3> ii3 = threeLegConnections.filter(new ThreeLegInternationalFilter())
                    .groupBy("f0.f0.f0", "f0.f2.f0", "f0.f4", "f1.f0.f0", "f1.f2.f0", "f1.f4", "f2.f0.f0", "f2.f2.f0", "f2.f4").reduceGroup(new CapacityReducer3())
                    .join(ODCaps, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f0.f2", "f7.f2").equalTo(0, 1).with(new CapShareJoiner3());

            ii1.writeAsCsv(outputPath + "itinerary/one", "\n", GeoInfo.DELIM, WriteMode.OVERWRITE);
            ii2.writeAsCsv(outputPath + "itinerary/two", "\n", GeoInfo.DELIM, WriteMode.OVERWRITE);
            ii3.writeAsCsv(outputPath + "itinerary/three", "\n", GeoInfo.DELIM, WriteMode.OVERWRITE);

            //env.execute("Phase 2");
            System.out.println(env.getExecutionPlan());
        } else if (phase == 3) {
            //
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull/");
            DataSet<Tuple2<Flight, Flight>> twoLegConnections = env.readFile(new FlightOutput.TwoLegFullInputFormat(), outputPath + "twoFull/");
            DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), outputPath + "threeFull/");

            // group keys: origin, destination, airline
            DataSet<Tuple4<GeoInfo, GeoInfo, String, Integer>> it1 = nonStopConnections.map(new MapFunction<Flight, Tuple4<GeoInfo, GeoInfo, String, Integer>>() {

                SimpleDateFormat format = new SimpleDateFormat("dd");

                @Override
                public Tuple4<GeoInfo, GeoInfo, String, Integer> map(Flight value) throws Exception {
                    Date date = new Date(value.getDepartureTimestamp());
                    String day = format.format(date);
                    return new Tuple4<GeoInfo, GeoInfo, String, Integer>(value.f0, value.f2, day, value.getMaxCapacity());
                }
            }).groupBy("f0.f0", "f1.f0", "f2").sum(3);


            // group keys: origin, destination, airline, origin, destination, airline
            DataSet<Tuple4<GeoInfo, GeoInfo, GeoInfo, GeoInfo>> it2 = twoLegConnections.map(new MapFunction<Tuple2<Flight, Flight>, Tuple4<GeoInfo, GeoInfo, GeoInfo, GeoInfo>>() {
                @Override
                public Tuple4<GeoInfo, GeoInfo, GeoInfo, GeoInfo> map(Tuple2<Flight, Flight> value) throws Exception {
                    return new Tuple4<GeoInfo, GeoInfo, GeoInfo, GeoInfo>(
                            value.f0.f0, value.f0.f2, value.f1.f0, value.f1.f2
                    );
                }
            }).distinct("f0.f0", "f1.f0", "f2.f0", "f3.f0");

            DataSet<Tuple6<GeoInfo, GeoInfo, GeoInfo, GeoInfo, GeoInfo, GeoInfo>> it3 = threeLegConnections.map(new MapFunction<Tuple3<Flight, Flight, Flight>, Tuple6<GeoInfo, GeoInfo, GeoInfo, GeoInfo, GeoInfo, GeoInfo>>() {
                @Override
                public Tuple6<GeoInfo, GeoInfo, GeoInfo, GeoInfo, GeoInfo, GeoInfo> map(Tuple3<Flight, Flight, Flight> value) throws Exception {
                    return new Tuple6<GeoInfo, GeoInfo, GeoInfo, GeoInfo, GeoInfo, GeoInfo>(
                            value.f0.f0, value.f0.f2, value.f1.f0, value.f1.f2, value.f2.f0, value.f2.f2
                    );
                }
            }).distinct("f0.f0", "f1.f0", "f2.f0", "f3.f0", "f4.f0", "f5.f0");


            it1.writeAsCsv(outputPath + "routing/one", "\n", ",", WriteMode.OVERWRITE).setParallelism(1);
            it2.writeAsCsv(outputPath + "routing/two", "\n", ",", WriteMode.OVERWRITE).setParallelism(1);
            it3.writeAsCsv(outputPath + "routing/three", "\n", ",", WriteMode.OVERWRITE).setParallelism(1);

            env.execute("Phase 3");
        } else if (phase == 4) {
            final long WAITING_FACTOR = 1L;

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull");
            DataSet<Tuple2<Flight, Flight>> twoLegConnections = env.readFile(new FlightOutput.TwoLegFullInputFormat(), outputPath + "twoFull");
            DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), outputPath + "threeFull");

            DataSet<Tuple8<String, Double, Double, String, Double, Double, String, Integer>> nonStop = nonStopConnections.map(new MapFunction<Flight, Tuple8<String, Double, Double, String, Double, Double, String, Integer>>() {
                SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

                @Override
                public Tuple8<String, Double, Double, String, Double, Double, String, Integer> map(Flight value) throws Exception {
                    Date date = new Date(value.getDepartureTimestamp());
                    String dayString = format.format(date);
                    long duration = value.getArrivalTimestamp() - value.getDepartureTimestamp();
                    if(duration <= 0L)
                        throw new Exception("Value error: " + value.toString());
                    Integer minutes = (int) (duration / (60L * 1000L));
                    return new Tuple8<String, Double, Double, String, Double, Double, String, Integer>
                            (value.getOriginAirport(), value.getOriginLatitude(), value.getOriginLongitude(), value.getDestinationAirport(), value.getDestinationLatitude(), value.getDestinationLongitude(), dayString, minutes);
                }
            });

            DataSet<Tuple8<String, Double, Double, String, Double, Double, String, Integer>> twoLeg = twoLegConnections.map(new MapFunction<Tuple2<Flight, Flight>, Tuple8<String, Double, Double, String, Double, Double, String, Integer>>() {
                SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

                @Override
                public Tuple8<String, Double, Double, String, Double, Double, String, Integer> map(Tuple2<Flight, Flight> value) throws Exception {
                    Date date = new Date(value.f0.getDepartureTimestamp());
                    String dayString = format.format(date);
                    long flight1 = value.f0.getArrivalTimestamp() - value.f0.getDepartureTimestamp();
                    long wait1 = WAITING_FACTOR * (value.f1.getDepartureTimestamp() - value.f0.getArrivalTimestamp());
                    long flight2 = value.f1.getArrivalTimestamp() - value.f1.getDepartureTimestamp();
                    long duration = flight1 + wait1 + flight2;
                    if(duration <= 0L)
                        throw new Exception("Value error: " + value.toString());
                    Integer minutes = (int) (duration / (60L * 1000L));
                    return new Tuple8<String, Double, Double, String, Double, Double, String, Integer>
                            (value.f0.getOriginAirport(), value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f1.getDestinationAirport(), value.f1.getDestinationLatitude(), value.f1.getDestinationLongitude(), dayString, minutes);
                }
            });

            DataSet<Tuple8<String, Double, Double, String, Double, Double, String, Integer>> threeLeg = threeLegConnections.map(new MapFunction<Tuple3<Flight, Flight, Flight>, Tuple8<String, Double, Double, String, Double, Double, String, Integer>>() {
                SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

                @Override
                public Tuple8<String, Double, Double, String, Double, Double, String, Integer> map(Tuple3<Flight, Flight, Flight> value) throws Exception {
                    Date date = new Date(value.f0.getDepartureTimestamp());
                    String dayString = format.format(date);
                    long flight1 = value.f0.getArrivalTimestamp() - value.f0.getDepartureTimestamp();
                    long wait1 = WAITING_FACTOR * (value.f1.getDepartureTimestamp() - value.f0.getArrivalTimestamp());
                    long flight2 = value.f1.getArrivalTimestamp() - value.f1.getDepartureTimestamp();
                    long wait2 = WAITING_FACTOR * (value.f2.getDepartureTimestamp() - value.f1.getArrivalTimestamp());
                    long flight3 = value.f2.getArrivalTimestamp() - value.f2.getDepartureTimestamp();
                    long duration = flight1 + wait1 + flight2 + wait2 + flight3;
                    if(duration <= 0L)
                        throw new Exception("Value error: " + value.toString());
                    Integer minutes = (int) (duration / (60L * 1000L));
                    return new Tuple8<String, Double, Double, String, Double, Double, String, Integer>
                            (value.f0.getOriginAirport(), value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f2.getDestinationAirport(), value.f2.getDestinationLatitude(), value.f2.getDestinationLongitude(), dayString, minutes);
                }
            });

            DataSet<Tuple8<String, Double, Double, String, Double, Double, String, Integer>> flights = nonStop.union(twoLeg).union(threeLeg);
            DataSet<Tuple11<String, Double, Double, String, Double, Double, String, Integer, Integer, Double, Integer>> result = flights.groupBy(0, 1, 2, 3, 4, 5, 6).reduceGroup(new GroupReduceFunction<Tuple8<String, Double, Double, String, Double, Double, String, Integer>, Tuple11<String, Double, Double, String, Double, Double, String, Integer, Integer, Double, Integer>>() {
                @Override
                public void reduce(Iterable<Tuple8<String, Double, Double, String, Double, Double, String, Integer>> values, Collector<Tuple11<String, Double, Double, String, Double, Double, String, Integer, Integer, Double, Integer>> out) throws Exception {
                    Tuple11<String, Double, Double, String, Double, Double, String, Integer, Integer, Double, Integer> result =
                            new Tuple11<String, Double, Double, String, Double, Double, String, Integer, Integer, Double, Integer>();
                    int min = Integer.MAX_VALUE;
                    int max = Integer.MIN_VALUE;
                    int sum = 0;
                    int count = 0;
                    boolean first = true;
                    for (Tuple8<String, Double, Double, String, Double, Double, String, Integer> t : values) {
                        if (first) {
                            result.f0 = t.f0;
                            result.f1 = t.f1;
                            result.f2 = t.f2;
                            result.f3 = t.f3;
                            result.f4 = t.f4;
                            result.f5 = t.f5;
                            result.f6 = t.f6;
                            first = false;
                        }
                        int minutes = t.f7;
                        sum += minutes;
                        count++;
                        if (minutes < min) {
                            min = minutes;
                        }
                        if (minutes > max) {
                            max = minutes;
                        }
                    }
                    Double avg = (double) sum / (double) count;
                    result.f7 = min;
                    result.f8 = max;
                    result.f9 = avg;
                    result.f10 = count;
                    out.collect(result);
                }
            });

            result.writeAsCsv(outputPath + "gravity", "\n", ",", WriteMode.OVERWRITE);
            env.execute("Phase 4");
        } else if (phase == 5) {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull");

            DataSet<Tuple7<String, Double, Double, String, Integer, Integer, Boolean>> inOutCapa = nonStopConnections.flatMap(new FlatMapFunction<Flight, Tuple7<String, Double, Double, String, Integer, Integer, Boolean>>() {
                SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

                @Override
                public void flatMap(Flight flight, Collector<Tuple7<String, Double, Double, String, Integer, Integer, Boolean>> out) throws Exception {
                    if(flight.getLegCount() > 1) {
                        return;
                    }
                    Date date = new Date(flight.getDepartureTimestamp());
                    String dayString = format.format(date);
                    boolean isInternational = !flight.getOriginCountry().equals(flight.getDestinationCountry());
                    Tuple7<String, Double, Double, String, Integer, Integer, Boolean> outgoing = new Tuple7<String, Double, Double, String, Integer, Integer, Boolean>
                    (flight.getOriginAirport(), flight.getOriginLatitude(), flight.getOriginLongitude(), dayString, flight.getMaxCapacity(), 0, isInternational);
                    Tuple7<String, Double, Double, String, Integer, Integer, Boolean> incoming = new Tuple7<String, Double, Double, String, Integer, Integer, Boolean>
                    (flight.getDestinationAirport(), flight.getDestinationLatitude(), flight.getDestinationLongitude(), dayString, 0, flight.getMaxCapacity(), isInternational);
                    out.collect(incoming);
                    out.collect(outgoing);
                }
            });

            DataSet<Tuple7<String, Double, Double, String, Integer, Integer, Boolean>> result = inOutCapa.groupBy(0,3,6).reduceGroup(new GroupReduceFunction<Tuple7<String, Double, Double, String, Integer, Integer, Boolean>, Tuple7<String, Double, Double, String, Integer, Integer, Boolean>>() {
                @Override
                public void reduce(Iterable<Tuple7<String, Double, Double, String, Integer, Integer, Boolean>> tuple7s,
                                   Collector<Tuple7<String, Double, Double, String, Integer, Integer, Boolean>> out) throws Exception {
                    boolean first = true;
                    Tuple7<String, Double, Double, String, Integer, Integer, Boolean> result = new Tuple7<String, Double, Double, String, Integer, Integer, Boolean>();
                    for(Tuple7<String, Double, Double, String, Integer, Integer, Boolean> t : tuple7s) {
                        if(first) {
                            result.f0 = t.f0;
                            result.f1 = t.f1;
                            result.f2 = t.f2;
                            result.f3 = t.f3;
                            result.f4 = 0;
                            result.f5 = 0;
                            result.f6 = t.f6;
                            first = false;
                        }
                        result.f4 += t.f4;
                        result.f5 += t.f5;
                    }
                    out.collect(result);
                }
            });

            result.writeAsCsv(outputPath + "loads", "\n", ",", WriteMode.OVERWRITE);
            env.execute("Phase 5");
        } else if(phase == 6) {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple4<String, String, Integer, Integer>> loads = env.readCsvFile(outputPath + "loads")
                    .includeFields(true, false, false, true, true, true, false)
                    .types(String.class, String.class, Integer.class, Integer.class);

            DataSet<Tuple4<String, String, String, Integer>> distance = env.readCsvFile(outputPath + "gravity")
                    .includeFields(true, false, false, true, false, false, true, true, false, false, false)
                    .types(String.class, String.class, String.class, Integer.class);

            DataSet<Tuple4<String, String, Integer, Integer>> agg = loads.groupBy(0, 1).sum(2).andSum(3);

            DataSet<Tuple3<String, String, Integer>> outgoing = agg.groupBy(0,1).sum(2).project(0,1,2);
            DataSet<Tuple3<String, String, Integer>> incoming = agg.groupBy(0,1).sum(3).project(0,1,3);

            DataSet<Tuple2<String, Integer>> outgoingSum = outgoing.groupBy(1).sum(2).project(1,2);

            outgoingSum.writeAsCsv(outputPath + "outsum", "\n", ",", WriteMode.OVERWRITE).setParallelism(1);

            DataSet<Tuple4<String, String, String, Long>> trafficMatrixIn = distance.join(incoming).where(0,2).equalTo(0,1)
                    .with(new JoinFunction<Tuple4<String, String, String, Integer>, Tuple3<String, String, Integer>, Tuple4<String, String, String, Long>>() {
                 @Override
                 public Tuple4<String, String, String, Long> join(Tuple4<String, String, String, Integer> distance, Tuple3<String, String, Integer> incoming) throws Exception {
                     Tuple4<String, String, String, Long> result = new Tuple4<String, String, String, Long>();
                     result.f0 = distance.f0;
                     result.f1 = distance.f1;
                     result.f2 = distance.f2;
                     result.f3 = (long) incoming.f2;
                     return result;
                 }
             });

            DataSet<Tuple4<String, String, String, Long>> trafficMatrixOut = trafficMatrixIn.join(outgoing).where(1,2).equalTo(0,1)
                    .with(new JoinFunction<Tuple4<String, String, String, Long>, Tuple3<String, String, Integer>, Tuple4<String, String, String, Long>>() {
                @Override
                public Tuple4<String, String, String, Long> join(Tuple4<String, String, String, Long> tmIn, Tuple3<String, String, Integer> outgoing) throws Exception {
                    Tuple4<String, String, String, Long> result = new Tuple4<String, String, String, Long>();
                    result.f0 = tmIn.f0;
                    result.f1 = tmIn.f1;
                    result.f2 = tmIn.f2;
                    result.f3 = tmIn.f3 * (long) outgoing.f2;
                    return result;
                }
            });

            DataSet<Tuple4<String, String, String, Double>> trafficMatrix = trafficMatrixOut.joinWithTiny(outgoingSum).where(2).equalTo(0)
                    .with(new JoinFunction<Tuple4<String, String, String, Long>, Tuple2<String, Integer>, Tuple4<String, String, String, Double>>() {
                @Override
                public Tuple4<String, String, String, Double> join(Tuple4<String, String, String, Long> tmOut, Tuple2<String, Integer> total) throws Exception {
                    Tuple4<String, String, String, Double> result = new Tuple4<String, String, String, Double>();
                    result.f0 = tmOut.f0;
                    result.f1 = tmOut.f1;
                    result.f2 = tmOut.f2;
                    result.f3 = tmOut.f3/(double)total.f1;
                    return result;
                }
            });

            trafficMatrix.writeAsCsv(outputPath + "tm", "\n", ",", WriteMode.OVERWRITE);
            env.execute("Phase 6");
        } else if(phase == 7) {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple4<String, String, Integer, Integer>> loads = env.readCsvFile(outputPath + "loads")
                    .includeFields(true, false, false, true, true, true, false)
                    .types(String.class, String.class, Integer.class, Integer.class);

            DataSet<Tuple8<String, Double, Double, String, Double, Double, String, Integer>> distance = env.readCsvFile(outputPath + "gravity")
                    .includeFields(true, true, true, true, true, true, true, true, false, false, false)
                    .types(String.class, Double.class, Double.class, String.class, Double.class, Double.class, String.class, Integer.class);

            DataSet<Tuple5<String, String, String, Integer, Double>> distanceReduced = distance
                    .map(new MapFunction<Tuple8<String, Double, Double, String, Double, Double, String, Integer>, Tuple5<String, String, String, Integer, Double>>() {
                        @Override
                        public Tuple5<String, String, String, Integer, Double> map(Tuple8<String, Double, Double, String, Double, Double, String, Integer> distance) throws Exception {
                            Tuple5<String, String, String, Integer, Double> result =
                            new Tuple5<String, String, String, Integer, Double>
                            (distance.f0, distance.f3, distance.f6, distance.f7, dist(distance.f1, distance.f2, distance.f4, distance.f5));
                            return result;
                        }
                    });

            DataSet<Tuple4<String, String, Integer, Integer>> agg = loads.groupBy(0, 1).sum(2).andSum(3);

            agg.writeAsCsv(outputPath + "agg", "\n", ",", WriteMode.OVERWRITE).setParallelism(1);

            DataSet<Tuple3<String, String, Integer>> outgoing = agg.groupBy(0,1).sum(2).project(0,1,2);
            DataSet<Tuple3<String, String, Integer>> incoming = agg.groupBy(0,1).sum(3).project(0,1,3);

            DataSet<Tuple4<String, String, String, Long>> trafficMatrixIn = distanceReduced.join(incoming, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0,2).equalTo(0,1)
                    .with(new JoinFunction<Tuple5<String, String, String, Integer, Double>, Tuple3<String, String, Integer>, Tuple4<String, String, String, Long>>() {
                        @Override
                        public Tuple4<String, String, String, Long> join(Tuple5<String, String, String, Integer, Double> distance, Tuple3<String, String, Integer> incoming) throws Exception {
                            Tuple4<String, String, String, Long> result = new Tuple4<String, String, String, Long>();
                            result.f0 = distance.f0;
                            result.f1 = distance.f1;
                            result.f2 = distance.f2;
                            result.f3 = (long) incoming.f2;
                            return result;
                        }
                    });

            DataSet<Tuple4<String, String, String, Long>> trafficMatrixOut = trafficMatrixIn.join(outgoing, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(1,2).equalTo(0,1)
                    .with(new JoinFunction<Tuple4<String, String, String, Long>, Tuple3<String, String, Integer>, Tuple4<String, String, String, Long>>() {
                        @Override
                        public Tuple4<String, String, String, Long> join(Tuple4<String, String, String, Long> tmIn, Tuple3<String, String, Integer> outgoing) throws Exception {
                            Tuple4<String, String, String, Long> result = new Tuple4<String, String, String, Long>();
                            result.f0 = tmIn.f0;
                            result.f1 = tmIn.f1;
                            result.f2 = tmIn.f2;
                            result.f3 = tmIn.f3 * (long) outgoing.f2;
                            return result;
                        }
                    });

            DataSet<Tuple6<String, String, String, Long, Integer, Double>> trafficMatrix = trafficMatrixOut.join(distanceReduced, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0,1,2).equalTo(0,1,2)
                    .with(new JoinFunction<Tuple4<String, String, String, Long>, Tuple5<String, String, String, Integer, Double>, Tuple6<String, String, String, Long, Integer, Double>>() {
                        @Override
                        public Tuple6<String, String, String, Long, Integer, Double> join(Tuple4<String, String, String, Long> tmOut, Tuple5<String, String, String, Integer, Double> distance) throws Exception {
                            Tuple6<String, String, String, Long, Integer, Double> result = new Tuple6<String, String, String, Long, Integer, Double>();
                            result.f0 = tmOut.f0;
                            result.f1 = tmOut.f1;
                            result.f2 = tmOut.f2;
                            result.f3 = tmOut.f3;
                            result.f4 = distance.f3;
                            result.f5 = distance.f4;
                            return result;
                        }
                    });


            trafficMatrix.writeAsCsv(outputPath + "tm", "\n", ",", WriteMode.OVERWRITE).setParallelism(1);
            env.execute("Phase 7");
        } else if(phase == 8) {
            final long WAITING_FACTOR = 1L;

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull");
            DataSet<Tuple2<Flight, Flight>> twoLegConnections = env.readFile(new FlightOutput.TwoLegFullInputFormat(), outputPath + "twoFull");
            DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), outputPath + "threeFull");

            DataSet<Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer>> nonStop = nonStopConnections.map(new MapFunction<Flight, Tuple10<String, String, Double, Double, String, String, Double, Double, String, Integer>>() {
                SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

                @Override
                public Tuple10<String, String, Double, Double, String, String, Double, Double, String, Integer> map(Flight value) throws Exception {
                    Date date = new Date(value.getDepartureTimestamp());
                    String dayString = format.format(date);
                    long duration = value.getArrivalTimestamp() - value.getDepartureTimestamp();
                    if(duration <= 0L)
                        throw new Exception("Value error: " + value.toString());
                    Integer minutes = (int) (duration / (60L * 1000L));
                    return new Tuple10<String, String, Double, Double, String, String, Double, Double, String, Integer>
                            (value.getOriginAirport(), value.getOriginCountry(), value.getOriginLatitude(), value.getOriginLongitude(),
                             value.getDestinationAirport(), value.getDestinationCountry(), value.getDestinationLatitude(), value.getDestinationLongitude(), dayString, minutes);
                }
            });

            DataSet<Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer>> twoLeg = twoLegConnections.map(new MapFunction<Tuple2<Flight, Flight>, Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer>>() {
                SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

                @Override
                public Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer> map(Tuple2<Flight, Flight> value) throws Exception {
                    Date date = new Date(value.f0.getDepartureTimestamp());
                    String dayString = format.format(date);
                    long flight1 = value.f0.getArrivalTimestamp() - value.f0.getDepartureTimestamp();
                    long wait1 = WAITING_FACTOR * (value.f1.getDepartureTimestamp() - value.f0.getArrivalTimestamp());
                    long flight2 = value.f1.getArrivalTimestamp() - value.f1.getDepartureTimestamp();
                    long duration = flight1 + wait1 + flight2;
                    if(duration <= 0L)
                        throw new Exception("Value error: " + value.toString());
                    Integer minutes = (int) (duration / (60L * 1000L));
                    return new Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer>
                            (value.f0.getOriginAirport(), value.f0.getOriginCountry(), value.f0.getOriginLatitude(), value.f0.getOriginLongitude(),
                             value.f1.getDestinationAirport(), value.f1.getDestinationCountry(), value.f1.getDestinationLatitude(), value.f1.getDestinationLongitude(), dayString, minutes);
                }
            });

            DataSet<Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer>> threeLeg = threeLegConnections.map(new MapFunction<Tuple3<Flight, Flight, Flight>, Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer>>() {
                SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

                @Override
                public Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer> map(Tuple3<Flight, Flight, Flight> value) throws Exception {
                    Date date = new Date(value.f0.getDepartureTimestamp());
                    String dayString = format.format(date);
                    long flight1 = value.f0.getArrivalTimestamp() - value.f0.getDepartureTimestamp();
                    long wait1 = WAITING_FACTOR * (value.f1.getDepartureTimestamp() - value.f0.getArrivalTimestamp());
                    long flight2 = value.f1.getArrivalTimestamp() - value.f1.getDepartureTimestamp();
                    long wait2 = WAITING_FACTOR * (value.f2.getDepartureTimestamp() - value.f1.getArrivalTimestamp());
                    long flight3 = value.f2.getArrivalTimestamp() - value.f2.getDepartureTimestamp();
                    long duration = flight1 + wait1 + flight2 + wait2 + flight3;
                    if(duration <= 0L)
                        throw new Exception("Value error: " + value.toString());
                    Integer minutes = (int) (duration / (60L * 1000L));
                    return new Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer>
                            (value.f0.getOriginAirport(), value.f0.getOriginCountry(), value.f0.getOriginLatitude(), value.f0.getOriginLongitude(),
                             value.f2.getDestinationAirport(), value.f2.getDestinationCountry(), value.f2.getDestinationLatitude(), value.f2.getDestinationLongitude(), dayString, minutes);
                }
            });

            DataSet<Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer>> flights = nonStop.union(twoLeg).union(threeLeg);
            DataSet<Tuple10<String, String, String, String, String, Integer, Integer, Double, Integer, Double>> result = flights.groupBy(0, 1, 2, 3, 4, 5, 6, 7, 8).reduceGroup(new GroupReduceFunction<Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer>, Tuple10<String, String, String, String, String, Integer, Integer, Double, Integer, Double>>() {
                @Override
                public void reduce(Iterable<Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer>> values, Collector<Tuple10<String, String, String, String, String, Integer, Integer, Double, Integer, Double>> out) throws Exception {
                    Tuple10<String, String, String, String, String, Integer, Integer, Double, Integer, Double> result =
                            new Tuple10<String, String, String, String, String, Integer, Integer, Double, Integer, Double>();
                    int min = Integer.MAX_VALUE;
                    int max = Integer.MIN_VALUE;
                    int sum = 0;
                    int count = 0;
                    boolean first = true;
                    for (Tuple10<String,String, Double, Double, String, String, Double, Double, String, Integer> t : values) {
                        if (first) {
                            result.f0 = t.f0;
                            result.f1 = t.f1;
                            result.f2 = t.f4;
                            result.f3 = t.f5;
                            result.f4 = t.f8;
                            result.f9 = dist(t.f2, t.f3, t.f6, t.f7);
                            first = false;
                        }
                        int minutes = t.f9;
                        sum += minutes;
                        count++;
                        if (minutes < min) {
                            min = minutes;
                        }
                        if (minutes > max) {
                            max = minutes;
                        }
                    }
                    Double avg = (double) sum / (double) count;
                    result.f5 = min;
                    result.f6 = max;
                    result.f7 = avg;
                    result.f8 = count;
                    out.collect(result);
                }
            });

            result.writeAsCsv(outputPath + "gravity2", "\n", ",", WriteMode.OVERWRITE).setParallelism(1);

            DataSet<Tuple5<String, String, Integer, Integer, String>> loads = env.readCsvFile(outputPath + "loads")
                    .includeFields(true, false, false, true, true, true, true)
                    .types(String.class, String.class, Integer.class, Integer.class, String.class);

            loads.writeAsCsv(outputPath + "loadsF", "\n", ",", WriteMode.OVERWRITE).setParallelism(1);

            env.execute("Phase 8");
        } else {
            throw new Exception("Invalid parameter! phase: " + phase);
        }
        //System.out.println(env.getExecutionPlan());
        long end = System.currentTimeMillis();
        LOG.info("Job ended at {} milliseconds. Running Time: {}", end, end - start);
    }

    /* HELPER FUNCTIONS */

    public static int getMinCap(Tuple2<Flight, Flight> conn) {
        if (conn.f0.getMaxCapacity() < conn.f1.getMaxCapacity()) {
            return conn.f0.getMaxCapacity();
        } else {
            return conn.f1.getMaxCapacity();
        }
    }

    public static int getMinCap(Tuple3<Flight, Flight, Flight> conn) {
        if (conn.f0.getMaxCapacity() <= conn.f1.getMaxCapacity() && conn.f0.getMaxCapacity() <= conn.f2.getMaxCapacity()) {
            return conn.f0.getMaxCapacity();
        } else if (conn.f1.getMaxCapacity() <= conn.f0.getMaxCapacity() && conn.f1.getMaxCapacity() <= conn.f2.getMaxCapacity()) {
            return conn.f1.getMaxCapacity();
        } else {
            return conn.f2.getMaxCapacity();
        }
    }

    /*
     * distance between two coordinates in kilometers
	 */
    private static double dist(double lat1, double long1, double lat2, double long2) {
        double d2r = Math.PI / 180.0;
        double dlong = (long2 - long1) * d2r;
        double dlat = (lat2 - lat1) * d2r;
        double a = Math.pow(Math.sin(dlat / 2.0), 2.0)
                + Math.cos(lat1 * d2r)
                * Math.cos(lat2 * d2r)
                * Math.pow(Math.sin(dlong / 2.0), 2.0);
        double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));
        double d = 6367.0 * c;
        return d;
    }

    /**
     * checks geoDetour for a two-leg flight
     *
     * @param originLatitude
     * @param originLongitude
     * @param hubLatitude
     * @param hubLongitude
     * @param destLatitude
     * @param destLongitude
     * @return true if acceptable, otherwise false
     * @throws Exception
     */
    private static boolean geoDetourAcceptable(double originLatitude, double originLongitude,
                                               double hubLatitude, double hubLongitude,
                                               double destLatitude, double destLongitude) throws Exception {
        double ODDist = dist(originLatitude, originLongitude, destLatitude, destLongitude);
        double legDistanceSum = 0.0;
        legDistanceSum += dist(originLatitude, originLongitude, hubLatitude, hubLongitude);
        legDistanceSum += dist(hubLatitude, hubLongitude, destLatitude, destLongitude);
        return (legDistanceSum / ODDist) <= computeMaxGeoDetour(ODDist);
    }

    /**
     * checks geoDetour for a three-leg flight
     *
     * @param originLatitude
     * @param originLongitude
     * @param hub1Latitude
     * @param hub1Longitude
     * @param hub2Latitude
     * @param hub2Longitude
     * @param destLatitude
     * @param destLongitude
     * @return true if acceptable, otherwise false
     * @throws Exception
     */
    private static boolean geoDetourAcceptable(double originLatitude, double originLongitude,
                                               double hub1Latitude, double hub1Longitude,
                                               double hub2Latitude, double hub2Longitude,
                                               double destLatitude, double destLongitude) throws Exception {
        double ODDist = dist(originLatitude, originLongitude, destLatitude, destLongitude);
        double legDistanceSum = 0.0;
        legDistanceSum += dist(originLatitude, originLongitude, hub1Latitude, hub1Longitude);
        legDistanceSum += dist(hub1Latitude, hub1Longitude, hub2Latitude, hub2Longitude);
        legDistanceSum += dist(hub2Latitude, hub2Longitude, destLatitude, destLongitude);
        return (legDistanceSum / ODDist) <= computeMaxGeoDetour(ODDist);
    }

    private static double computeMaxGeoDetour(double ODDist) {
        return Math.min(1.5, -0.365 * Math.log(ODDist) + 4.8);
    }

    private static long computeMaxCT(double ODDist) {
        return (long) (180.0 * Math.log(ODDist + 1000.0) - 1000.0);
    }

    private static long computeMinCT() {
        return MCT_MIN;
    }

    private static boolean isDomestic(Flight f) {
        return f.getOriginCountry().equals(f.getDestinationCountry());
    }

    private static boolean isODDomestic(Flight in1, Flight in2) {
        return in1.getOriginCountry().equals(in2.getDestinationCountry());
    }

    private static boolean isFullyDomestic(Flight in1, Flight in2) {
        return in1.getOriginCountry().equals(in2.getDestinationCountry()) && in1.getDestinationCountry().equals(in1.getOriginCountry());
    }

    private static double travelTimeAt100kphInMinutes(double ODDist) {
        return (ODDist * 60.0) / 100.0;
    }

    /**
     * Class graveyard
     */

    /**
     * Statistics for non-stop flights
     */
    public static class ODCounter1 implements GroupReduceFunction<Flight, ConnectionStats> {

        @Override
        public void reduce(Iterable<Flight> values, Collector<ConnectionStats> out) throws Exception {
            Iterator<Flight> iter = values.iterator();
            Flight flight = iter.next().clone();
            ConnectionStats output = new ConnectionStats();
            output.f0 = flight.getOriginAirport();
            output.f1 = flight.getDestinationAirport();
            int count = 1;
            int invalid = 0;
            int cap = -1;
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            int sum = 0;
            cap = flight.getMaxCapacity();
            if (cap > 0) {
                sum += cap;
                if (cap < min) {
                    min = cap;
                }
                if (cap > max) {
                    max = cap;
                }
            } else {
                invalid++;
            }
            while (iter.hasNext()) {
                flight = iter.next().clone();
                count++;
                cap = flight.getMaxCapacity();
                if (cap > 0) {
                    sum += cap;
                    if (cap < min) {
                        min = cap;
                    }
                    if (cap > max) {
                        max = cap;
                    }
                } else {
                    invalid++;
                }
            }
            output.f2 = count;
            output.f3 = max;
            output.f4 = min;
            output.f5 = sum;
            output.f6 = invalid;
            output.f7 = 1;
            out.collect(output);
        }
    }

    /**
     * Statistics for two-leg flights
     */
    public static class ODCounter2 implements GroupReduceFunction<Tuple2<Flight, Flight>, ConnectionStats> {

        @Override
        public void reduce(Iterable<Tuple2<Flight, Flight>> values, Collector<ConnectionStats> out) throws Exception {
            Iterator<Tuple2<Flight, Flight>> iter = values.iterator();
            Tuple2<Flight, Flight> flight;
            ConnectionStats output = new ConnectionStats();
            int count = 0;
            int invalid = 0;
            int cap = -1;
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            int sum = 0;
            do {
                flight = iter.next().copy();
                count++;
                cap = getMinCap(flight);
                if (cap > 0) {
                    sum += cap;
                    if (cap < min) {
                        min = cap;
                    }
                    if (cap > max) {
                        max = cap;
                    }
                } else {
                    invalid++;
                }
            } while (iter.hasNext());
            output.f0 = flight.f0.getOriginAirport();
            output.f1 = flight.f1.getDestinationAirport();
            output.f2 = count;
            output.f3 = max;
            output.f4 = min;
            output.f5 = sum;
            output.f6 = invalid;
            output.f7 = 2;
            out.collect(output);
        }
    }

    /**
     * Statistics for three-leg flights
     */
    public static class ODCounter3 implements GroupReduceFunction<Tuple3<Flight, Flight, Flight>, ConnectionStats> {

        @Override
        public void reduce(Iterable<Tuple3<Flight, Flight, Flight>> values, Collector<ConnectionStats> out) throws Exception {
            Iterator<Tuple3<Flight, Flight, Flight>> iter = values.iterator();
            Tuple3<Flight, Flight, Flight> flight;
            ConnectionStats output = new ConnectionStats();
            int count = 0;
            int invalid = 0;
            int cap = -1;
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            int sum = 0;
            do {
                flight = iter.next().copy();
                count++;
                cap = getMinCap(flight);
                if (cap > 0) {
                    sum += cap;
                    if (cap < min) {
                        min = cap;
                    }
                    if (cap > max) {
                        max = cap;
                    }
                } else {
                    invalid++;
                }
            } while (iter.hasNext());
            output.f0 = flight.f0.getOriginAirport();
            output.f1 = flight.f2.getDestinationAirport();
            output.f2 = count;
            output.f3 = max;
            output.f4 = min;
            output.f5 = sum;
            output.f6 = invalid;
            output.f7 = 3;
            out.collect(output);
        }
    }
}

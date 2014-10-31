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
import java.util.logging.Filter;

public class FlightConnectionJoiner {

    public static final Logger LOG = LoggerFactory.getLogger(FlightConnectionJoiner.class);

	public static final long START 	= 1398902400000L;//1398902400000L;
	public static final long END 	= 1401580800000L;//1399507200000L;//1399020800000L;//

    public static final long MCT_MIN = 10L;
    //public static final long MCT_MAX = 551L;

    public static final Character EMPTY = new Character(' ');

    public static final long WINDOW_SIZE = 48L * 60L * 60L * 1000L; // 48 hours
    public static final int MAX_WINDOW_ID = (int)Math.ceil((2.0*(END-START))/(double)WINDOW_SIZE);

    public static int getFirstWindow(long timestamp) {
        int windowIndex = (int)(2L*(timestamp-START) / WINDOW_SIZE);
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
	//origin, destination, airline, flight number, departure, arrival, originLat, originLong, originCountry, destinationLat, destinationLong, destinationCountry, originTerminal, destinationTerminal, codeshareInfo

	@SuppressWarnings("serial")
	public static class FilteringUTCExtractor implements 
	FlatMapFunction<String, Flight> {
		
		String[] tmp = null;
		
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+0000"), Locale.FRENCH);
		
		SimpleDateFormat format = new SimpleDateFormat("yyMMddHHmm");
		
		Date earliest = new Date(START);
		Date latest = new Date(END);
		
		private final static ArrayList<String> nonAirCraftList = new ArrayList<String>() {{
			add("AGH");add("BH2");add("BUS");add("ICE");
			add("LCH");add("LMO");add("MD9");add("NDE");
			add("S61");add("S76");add("TRN");add("TSL");
			}};
		
		public void flatMap(String value, Collector<Flight> out) throws Exception {
            // TODO: check for service type SSIM p.483
			tmp = value.split("\\^");
            if(tmp[0].trim().startsWith("#")) {
                // header
                return;
            }
            String aircraft = tmp[9].trim();
            if(nonAirCraftList.contains(aircraft)) {
                // not an aircraft
                return;
            }
			String localDepartureDate = tmp[0].trim();
			String localDepartureTime = tmp[3].trim();
			String localDepartureOffset = tmp[4].trim();
			Date departure = format.parse(localDepartureDate + localDepartureTime);
			cal.setTime(departure);
			int sign = 0;
			if(localDepartureOffset.startsWith("-")) {
				sign = 1;
			} else if(localDepartureOffset.startsWith("+")) {
				sign = -1;
			} else {
				throw new Exception("Parse error. Wrong sign! Original String: " + value);
			}
			int hours = Integer.parseInt(localDepartureOffset.substring(1, 3));
			int minutes = Integer.parseInt(localDepartureOffset.substring(3, 5));
			cal.add(Calendar.HOUR_OF_DAY, sign*hours);
			cal.add(Calendar.MINUTE, sign*minutes);
			departure = cal.getTime();
			if(departure.before(earliest) || departure.after(latest)) {
				return;
			}
			if(tmp.length > 37 && !tmp[37].trim().isEmpty()) {
				// not an operating carrier
				return;
			}
			String localArrivalTime = tmp[5].trim();
			String localArrivalOffset = tmp[6].trim();
			String dateChange = tmp[7].trim();
			Date arrival = format.parse(localDepartureDate + localArrivalTime);
			cal.setTime(arrival);
			if(!dateChange.equals("0")) {
				if(dateChange.equals("1")) {
					cal.add(Calendar.DAY_OF_YEAR, 1);
				} else if(dateChange.equals("2")) {
					cal.add(Calendar.DAY_OF_YEAR, 2);
				} else if(dateChange.equals("A") || dateChange.equals("J") || dateChange.equals("-1")) {
					cal.add(Calendar.DAY_OF_YEAR, -1);
				} else {
					throw new Exception("Unknown arrival_date_variation modifier: " + dateChange + " original string: " + value);
				}
			}
			if(localArrivalOffset.startsWith("-")) {
				sign = 1;
			} else if(localArrivalOffset.startsWith("+")) {
				sign = -1;
			} else {
				throw new Exception("Parse error. Wrong sign!");
			}
			hours = Integer.parseInt(localArrivalOffset.substring(1, 3));
			minutes = Integer.parseInt(localArrivalOffset.substring(3, 5));
			cal.add(Calendar.HOUR_OF_DAY, sign*hours);
			cal.add(Calendar.MINUTE, sign*minutes);
			arrival = cal.getTime();
			// sanity check
			if(arrival.before(departure) || arrival.equals(departure)) {
				return;
				/*throw new Exception("Sanity check failed! Arrival equal to or earlier than departure.\n" +
						"Departure: " + departure.toString() + " Arrival: " + arrival.toString() + "\n"  + 
						"Sign: " + sign + " Hours: " + hours + " Minutes: " + minutes + "\n" +
						"Original value: " + value);*/
			}
            String codeshareInfo = "";
            if(tmp.length > 36) {
                codeshareInfo = tmp[36].trim();
                if(codeshareInfo.length() > 2) {
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
            if(!tmp[12].trim().isEmpty()) {
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
            if(tmp[0].trim().startsWith("#")) {
                // header
                return;
            }
			if(!tmp[41].trim().equals("A")) {
				// not an airport
				return;
			}
			from = tmp[13].trim();
			until = tmp[14].trim();
			if(!until.equals("")) {
				// expired
				return;
			}
			Double latitude;
			Double longitude;
			try {
				latitude = new Double(Double.parseDouble(tmp[8].trim()));
				longitude = new Double(Double.parseDouble(tmp[9].trim()));
			} catch(Exception e) {
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
            if(tmp[8].trim().equals("1")) {
                regionCode = "SCH";
            } else {
                regionCode = tmp[6].trim().replace("\"","");
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
            if(!second.f1.isEmpty()) {
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

            if(!second.f1.isEmpty()) {
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
            if(!second.f1.isEmpty()) {
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
            if(!in1.getDestinationCity().equals(in2.getOriginCity())) {
                throw new Exception("Hub city mismatch: " + in1.toString() + " / " + in2.toString());
            }
            if(!in1.getAirline().equals(in2.getAirline()) && in1.getFlightNumber().equals(in2.getFlightNumber())) {
                throw new Exception("Flight mismatch: " + in1.toString() + " / " + in2.toString());
            }
            if(exceptionsIn.indexOf(in1.getTrafficRestriction()) >= 0) {
                return;
            }
            if(((exceptions.indexOf(in1.getTrafficRestriction()) >= 0) && in2.getTrafficRestriction().equals(EMPTY)) ||
               ((exceptions.indexOf(in2.getTrafficRestriction()) >= 0) && in1.getTrafficRestriction().equals(EMPTY)) ) {
                return;
            }
            long hubTime = in2.getDepartureTimestamp() - in1.getArrivalTimestamp();
            long travelTime = in2.getArrivalTimestamp() - in1.getDepartureTimestamp();
            if(hubTime <= 0L) {
                // arrival before departure
                return;
            }
            if(hubTime < (computeMinCT()*60L*1000L)) {
                return;
            }
            double ODDistance = dist(in1.getOriginLatitude(), in1.getOriginLongitude(), in2.getDestinationLatitude(), in2.getDestinationLongitude());
            if(hubTime > ((computeMaxCT(ODDistance))*60L*1000L)) {
                return;
            }
            if(in1.getOriginAirport().equals(in2.getDestinationAirport())) {
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
            if(!in1.getAircraftType().equals(in2.getAircraftType())) {
                if(in1.getMaxCapacity() <= in2.getMaxCapacity()) {
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
            if(!in1.getCodeShareInfo().isEmpty() && !in2.getCodeShareInfo().isEmpty()) {
                // merge codeshare info
                String[] codeshareInfo1 = in1.getCodeShareInfo().split("/");
                String[] codeshareInfo2 = in2.getCodeShareInfo().split("/");
                for (int i = 0; i < codeshareInfo1.length; i++) {
                    for (int j = 0; j < codeshareInfo2.length; j++) {
                        // keep all codeshare info that allows a connection over this multi-leg segment
                        if(codeshareInfo1[i].substring(0,2).equals(codeshareInfo2[j].substring(0,2))) {
                            codeshareInfo += codeshareInfo1[i] + "/";
                            codeshareInfo += codeshareInfo2[j] + "/";
                        }
                    }
                }
            }
            if(!codeshareInfo.isEmpty()) {
                codeshareInfo = codeshareInfo.substring(0,codeshareInfo.lastIndexOf('/'));
            }
            result.setCodeShareInfo(codeshareInfo);

            if(in1.getTrafficRestriction().equals('I') && in2.getTrafficRestriction().equals('I')) {
                result.setTrafficRestriction(EMPTY);
            } else if(in1.getTrafficRestriction().equals('A') && in2.getTrafficRestriction().equals('A')) {
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
			if(!in1.getDestinationCity().equals(in2.getOriginCity())) {
				throw new Exception("Hub city mismatch: " + in1.toString() + " / " + in2.toString());
			}
			long hubTime = in2.getDepartureTimestamp() - in1.getArrivalTimestamp();
            long travelTime = in2.getArrivalTimestamp() - in1.getDepartureTimestamp();
			if(hubTime <= 0L) {
				// arrival before departure
				return;
			}
			if(hubTime < (computeMinCT()*60L*1000L)) {
				return;
			}
            double ODDistance = dist(in1.getOriginLatitude(), in1.getOriginLongitude(), in2.getDestinationLatitude(), in2.getDestinationLongitude());
			if(hubTime > ((computeMaxCT(ODDistance))*60L*1000L)) {
				return;
			}
            if(in1.getOriginAirport().equals(in2.getDestinationAirport())) {
                // some multi-leg flights are circular
                return;
            }
            if(in1.getAirline().equals(in2.getAirline()) && in1.getFlightNumber().equals(in2.getFlightNumber())) {
                // multi-leg flight connections have already been built
                return;
            }
            // check traffic restrictions
            if((exceptionsGeneral.indexOf(in1.getTrafficRestriction()) >= 0) ||
               (exceptionsGeneral.indexOf(in2.getTrafficRestriction()) >= 0)) {
                return;
            } else if(!isDomestic(in2) && exceptionsDomestic.indexOf(in1.getTrafficRestriction()) >= 0) {
                return;
            } else if(!isDomestic(in1) && exceptionsDomestic.indexOf(in2.getTrafficRestriction()) >= 0) {
                return;
            } else if(isDomestic(in2) && exceptionsInternational.indexOf(in1.getTrafficRestriction()) >= 0) {
                return;
            } else if(isDomestic(in1) && exceptionsInternational.indexOf(in2.getTrafficRestriction()) >= 0) {
                return;
            }
            if(isODDomestic(in1, in2)) {
                if(!isDomestic(in1)) {
                    if(hubTime > 240L*60L*1000L ||
                       !geoDetourAcceptable(in1.getOriginLatitude(), in1.getOriginLongitude(),
                            in1.getDestinationLatitude(), in1.getDestinationLongitude(),
                            in2.getDestinationLatitude(), in2.getDestinationLongitude())) {
                        // for domestic connections with the hub in another country the MaxCT is 240 min
                        // and geoDetour applies
                        return;
                    }
                } else {
                    if(travelTime > (travelTimeAt100kphInMinutes(ODDistance)*60L*1000L)) {
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
            } else if(in1.getCodeShareInfo().isEmpty() && in2.getCodeShareInfo().isEmpty()) {
                // not the same airline and no codeshare information
                return;
            } else {
                // check if a connection can be made via codeshares
                String[] codeshareAirlines1 = null;
                if(!in1.getCodeShareInfo().isEmpty()) {
                    String[] codeshareInfo1 = in1.getCodeShareInfo().split("/");
                    codeshareAirlines1 = new String[codeshareInfo1.length+1];
                    for (int i = 0; i < codeshareInfo1.length; i++) {
                        codeshareAirlines1[i] = codeshareInfo1[i].substring(0,2);
                    }
                    codeshareAirlines1[codeshareAirlines1.length-1] = in1.getAirline();
                } else {
                    codeshareAirlines1 = new String[]{in1.getAirline()};
                }
                String[] codeshareAirlines2 = null;
                if(!in2.getCodeShareInfo().isEmpty()) {
                    String[] codeshareInfo2 = in2.getCodeShareInfo().split("/");
                    codeshareAirlines2 = new String[codeshareInfo2.length+1];
                    for (int i = 0; i < codeshareInfo2.length; i++) {
                        codeshareAirlines2[i] = codeshareInfo2[i].substring(0,2);
                    }
                    codeshareAirlines2[codeshareAirlines2.length-1] = in2.getAirline();
                } else {
                    codeshareAirlines2 = new String[]{in2.getAirline()};
                }
                for(int i = 0; i < codeshareAirlines1.length; i++) {
                    for(int j = 0; j < codeshareAirlines2.length; j++) {
                        if(codeshareAirlines1[i].equals(codeshareAirlines2[j])) {
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
        public void join(Tuple2<Flight, Flight> in1 ,Tuple2<Flight, Flight> in2, Collector<Tuple3<Flight, Flight, Flight>> out) throws Exception {

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
                if(dep.isEmpty()) {
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
                } else if(!arr.equals(dep)) {
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
                    if(match(conn, e)) {
                        bestRule = e;
                        break; // first hit is best rule since the list is sorted by score
                    }
                }
                if(bestRule != null) {
                    MCT = bestRule.getMCT();
                } else {
                    MCT = defaultMCT;
                }
                if(checkWithMCT(conn, MCT)) {
                    out.collect(conn);
                }
            }
            //if(LOG.isInfoEnabled()) {
            //    end = System.currentTimeMillis();
            //    ruleCheckingTimes.add(end-start);
            //}
        }

        private static boolean checkWithMCT(Tuple2<Flight, Flight> conn, long MCT) {
            if(MCT >= 999L) {
                return false;
            }
            long MinCT = MCT*60L*1000L;
            long hubTime = conn.f1.getDepartureTimestamp() - conn.f0.getArrivalTimestamp();
            if(hubTime < MinCT) {
                return false;
            } else {
                return true;
            }
        }

        public static class ScoreComparator implements Comparator<MCTEntry> {

            @Override
            public int compare(MCTEntry mctEntry, MCTEntry mctEntry2) {
                // descending order
                return scoreRule(mctEntry2)-scoreRule(mctEntry);
            }

            @Override
            public boolean equals(Object o) {
                // MCT rules are assumed to be unique
                return false;
            }
        }

        public static int scoreRule(MCTEntry rule) {
            int result = 0;
            if(rule.getStat().isEmpty())
                return 0;
            if(!rule.getDeparture().isEmpty())
                result += 1 << 0;
            if(!rule.getArrival().isEmpty())
                result += 1 << 1;
            if(!rule.getDepTerminal().isEmpty())
                result += 1 << 2;
            if(!rule.getArrTerminal().isEmpty())
                result += 1 << 3;
            if(!rule.getNextRegion().isEmpty())
                result += 1 << 4;
            if(!rule.getPrevRegion().isEmpty())
                result += 1 << 5;
            if(!rule.getNextCountry().isEmpty())
                result += 1 << 6;
            if(!rule.getPrevCountry().isEmpty())
                result += 1 << 7;
            if(!rule.getNextState().isEmpty())
                result += 1 << 8;
            if(!rule.getPrevState().isEmpty())
                result += 1 << 9;
            if(!rule.getNextCity().isEmpty())
                result += 1 << 10;
            if(!rule.getPrevCity().isEmpty())
                result += 1 << 11;
            if(!rule.getNextAirport().isEmpty())
                result += 1 << 12;
            if(!rule.getPrevAirport().isEmpty())
                result += 1 << 13;
            if(!rule.getDepAircraft().isEmpty())
                result += 1 << 14;
            if(!rule.getArrAircraft().isEmpty())
                result += 1 << 15;
            if(!rule.getDepCarrier().isEmpty()) {
                result += 1 << 16;
                if(rule.getOutFlightNumber().intValue() != 0) {
                    result += 1 << 18;
                }
            }
            if(!rule.getArrCarrier().isEmpty()) {
                result += 1 << 17;
                if(rule.getInFlightNumber().intValue() != 0) {
                    result += 1 << 19;
                }
            }
            return result;
        }

        private static boolean match(Tuple2<Flight, Flight> conn, MCTEntry rule) {
            if(!rule.getArrival().isEmpty()) {
                if(!conn.f0.getDestinationAirport().equals(rule.getArrival())) {
                   return false;
                }
            }
            if(!rule.getDeparture().isEmpty()) {
                if(!conn.f1.getLastAirport().equals(rule.getDeparture())) {
                    return false;
                }
            }
            if(!rule.getArrCarrier().isEmpty()) {
                if(!conn.f0.getAirline().equals(rule.getArrCarrier())) {
                    return false;
                } else {
                    if(rule.getInFlightNumber().intValue() != 0) {
                        if(!(conn.f0.getFlightNumber() >= rule.getInFlightNumber() &&
                             conn.f0.getFlightNumber() <= rule.getInFlightEOR())) {
                            return false;
                        }
                    }
                }
            }
            if(!rule.getDepCarrier().isEmpty()) {
                if(!conn.f1.getAirline().equals(rule.getDepCarrier())) {
                    return false;
                } else {
                    if(rule.getOutFlightNumber().intValue() != 0) {
                        if(!(conn.f1.getFlightNumber() >= rule.getOutFlightNumber() &&
                             conn.f1.getFlightNumber() <= rule.getOutFlightEOR())) {
                            return false;
                        }
                    }
                }
            }
            if(!rule.getArrAircraft().isEmpty()) {
                if(!conn.f0.getAircraftType().equals(rule.getArrAircraft())) {
                    return false;
                }
            }
            if(!rule.getDepAircraft().isEmpty()) {
                if(!conn.f1.getAircraftType().equals(rule.getDepAircraft())) {
                    return false;
                }
            }
            if(!rule.getArrTerminal().isEmpty()) {
                if(!conn.f0.getDestinationTerminal().equals(rule.getArrTerminal())) {
                    return false;
                }
            }
            if(!rule.getDepTerminal().isEmpty()) {
                if(!conn.f1.getLastTerminal().equals(rule.getDepTerminal())) {
                    return false;
                }
            }
            if(!rule.getPrevCountry().isEmpty()) {
                if(!conn.f0.getLastCountry().equals(rule.getPrevCountry())) {
                    return false;
                }
            }
            if(!rule.getPrevCity().isEmpty()) {
                if(!conn.f0.getLastCity().equals(rule.getPrevCity())) {
                    return false;
                }
            }
            if(!rule.getPrevAirport().isEmpty()) {
                if(!conn.f0.getLastAirport().equals(rule.getPrevAirport())) {
                    return false;
                }
            }
            if(!rule.getNextCountry().isEmpty()) {
                if(!conn.f1.getDestinationCountry().equals(rule.getNextCountry())) {
                    return false;
                }
            }
            if(!rule.getNextCity().isEmpty()) {
                if(!conn.f1.getDestinationCity().equals(rule.getNextCity())) {
                    return false;
                }
            }
            if(!rule.getNextAirport().isEmpty()) {
                if(!conn.f1.getDestinationAirport().equals(rule.getNextAirport())) {
                    return false;
                }
            }
            if(!rule.getPrevState().isEmpty()) {
                if(!conn.f0.getLastState().equals(rule.getPrevState())) {
                    return false;
                }
            }
            if(!rule.getNextState().isEmpty()) {
                if(!conn.f1.getDestinationState().equals(rule.getNextState())) {
                    return false;
                }
            }
            if(!rule.getPrevRegion().isEmpty()) {
                // SCH is a real subset of EUR
                if(!rule.getPrevRegion().equals("EUR") || !rule.getPrevRegion().equals("SCH")) {
                    if(!conn.f0.getLastRegion().equals(rule.getPrevRegion())) {
                        return false;
                    }
                } else {
                    if(!conn.f0.getLastRegion().equals("SCH") || !conn.f0.getLastRegion().equals("EUR")) {
                        return false;
                    }
                }
            }
            if(!rule.getNextRegion().isEmpty()) {
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
            if(flight.getDepartureTimestamp() < START || flight.getDepartureTimestamp() > END) {
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
            if(exceptions.indexOf(value.getTrafficRestriction()) >= 0) {
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
            if(exceptionsGeneral.indexOf(value.f0.getTrafficRestriction()) >= 0) {
                return false;
            } else if(!isDomestic(value.f1) && exceptionsDomestic.indexOf(value.f0.getTrafficRestriction()) >= 0) {
                return false;
            } else if(!isDomestic(value.f0) && exceptionsDomestic.indexOf(value.f1.getTrafficRestriction()) >= 0) {
                return false;
            } else if(isDomestic(value.f1) && exceptionsInternational.indexOf(value.f0.getTrafficRestriction()) >= 0) {
                return false;
            } else if(isDomestic(value.f0) && exceptionsInternational.indexOf(value.f1.getTrafficRestriction()) >= 0) {
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
            while(iter.hasNext()) {
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
                if(cap > 0) {
                    sum += cap;
                    if(cap < min) {
                        min = cap;
                    }
                    if(cap > max) {
                        max = cap;
                    }
                } else {
                    invalid++;
                }
            } while(iter.hasNext());
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

        private int getMinCap(Tuple2<Flight, Flight> conn) {
            if(conn.f0.getMaxCapacity() < conn.f1.getMaxCapacity()) {
                return conn.f0.getMaxCapacity();
            } else {
                return conn.f1.getMaxCapacity();
            }
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
                if(cap > 0) {
                    sum += cap;
                    if(cap < min) {
                        min = cap;
                    }
                    if(cap > max) {
                        max = cap;
                    }
                } else {
                    invalid++;
                }
            } while(iter.hasNext());
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

        private int getMinCap(Tuple3<Flight, Flight, Flight> conn) {
            if(conn.f0.getMaxCapacity() <= conn.f1.getMaxCapacity() && conn.f0.getMaxCapacity() <= conn.f2.getMaxCapacity()) {
                return conn.f0.getMaxCapacity();
            } else if(conn.f1.getMaxCapacity() <= conn.f0.getMaxCapacity() && conn.f1.getMaxCapacity() <= conn.f2.getMaxCapacity()){
                return conn.f1.getMaxCapacity();
            } else {
                return conn.f2.getMaxCapacity();
            }
        }
    }

	public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        LOG.info("Job started at {} milliseconds.", start);
        if(!parseParameters(args)) {
            return;
        }
        boolean first = false;
        if(args.length == 1) {
            first = Boolean.parseBoolean(args[0]);
        }
        // the program is split into two parts (building and parsing all non-stop connections, and finding multi-leg flights)
        if(first) {
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
            // discard all connections that don't contain an IATA airport code
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

            DataSet<Flight> join1 = extracted.join(airportCoordinates, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(jk1).equalTo(0).with(new OriginCoordinateJoiner());
            DataSet<Flight> join2 = join1.join(airportCoordinates, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(jk2).equalTo(0).with(new DestinationCoordinateJoiner());

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
            DataSet<Flight> multiLeg2 = join4.join(join4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5").equalTo("f0.f2", "f4", "f5").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg3 = multiLeg2.join(join4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5").equalTo("f0.f2", "f4", "f5").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg4 = multiLeg2.join(multiLeg2, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5").equalTo("f0.f2", "f4", "f5").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg5 = multiLeg2.join(multiLeg3, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5").equalTo("f0.f2", "f4", "f5").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg6 = multiLeg3.join(multiLeg3, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5").equalTo("f0.f2", "f4", "f5").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg7 = multiLeg3.join(multiLeg4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5").equalTo("f0.f2", "f4", "f5").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg8 = multiLeg4.join(multiLeg4, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f2.f2", "f4", "f5").equalTo("f0.f2", "f4", "f5").with(new MultiLegJoiner());

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

            FileOutputFormat nonStop = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath + "one/"));
            singleFltNoFlights.filter(new NonStopTrafficRestrictionsFilter()).write(nonStop, outputPath + "one/", WriteMode.OVERWRITE);

            FileOutputFormat nonStopFull = new FlightOutput.NonStopFullOutputFormat();
            singleFltNoFlights.write(nonStopFull, outputPath + "oneFull/", WriteMode.OVERWRITE);

            env.execute();
        } else {
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

            FileOutputFormat twoLeg = new FlightOutput.TwoLegFlightOutputFormat(new Path(outputPath + "two/"));
            twoLegConnectionsFiltered.write(twoLeg, outputPath + "two/", WriteMode.OVERWRITE);

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
                    = twoLegConnectionsFiltered.join(twoLegConnectionsFiltered, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("f1.f0.f0", "f1.f1", "f1.f2.f0", "f1.f4", "f1.f5").equalTo("f0.f0.f0", "f0.f1", "f0.f2.f0", "f0.f4", "f0.f5").with(new ThreeLegJoiner());

            FileOutputFormat threeLeg = new FlightOutput.ThreeLegFlightOutputFormat(new Path(outputPath + "three/"));
            threeLegConnections.write(threeLeg, outputPath + "three/", WriteMode.OVERWRITE);

            //DataSet<ConnectionStats> ODCounts1 = join4.groupBy(0,9).reduceGroup(new ODCounter1());
            //ODCounts1.writeAsText(outputPath+"counts/one/", WriteMode.OVERWRITE);

            //DataSet<ConnectionStats> ODCounts2 = twoLegConnectionsFiltered.groupBy("f0.f0", "f1.f9").reduceGroup(new ODCounter2());
            //ODCounts2.writeAsText(outputPath+"counts/two/", WriteMode.OVERWRITE);

            //DataSet<ConnectionStats> ODCounts3 = threeLegConnections.groupBy("f0.f0", "f2.f9").reduceGroup(new ODCounter3());
            //ODCounts3.writeAsText(outputPath+"counts/three/", WriteMode.OVERWRITE);

            //DataSet<ConnectionStats> union = ODCounts1.union(ODCounts2).union(ODCounts3);
            //union.writeAsText(outputPath+"counts/union/", WriteMode.OVERWRITE);

            //union.groupBy(0,1).sum(2).andMax(3).andMin(4).andSum(5).andSum(6).andMin(7).writeAsText(outputPath+"counts/aggregated/", WriteMode.OVERWRITE);

            env.execute();
        }
        //System.out.println(env.getExecutionPlan());
        long end = System.currentTimeMillis();
        LOG.info("Job ended at {} milliseconds. Running Time: {}", end, end-start);
	}

    /* HELPER FUNCTIONS */

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
        return (legDistanceSum/ODDist) <= computeMaxGeoDetour(ODDist);
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
        return (legDistanceSum/ODDist) <= computeMaxGeoDetour(ODDist);
    }

    private static double computeMaxGeoDetour(double ODDist) {
        return Math.min(1.5, -0.365*Math.log(ODDist)+4.8);
    }

    private static long computeMaxCT(double ODDist) {
        return (long) (180.0*Math.log(ODDist+1000.0)-1000.0);
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
        return (ODDist*60.0)/100.0;
    }


    private static String schedulePath;
    private static String oriPath;
    private static String regionPath;
    private static String defaultCapacityPath;
    private static String capacityPath;
    private static String mctPath;
    private static String outputPath;

    private static boolean parseParameters(String[] args) {
        if(args.length == 0) {
            schedulePath = "file:///home/robert/Amadeus/data/week/";//"hdfs:///user/rwaury/input/all_catalog_140410.txt";//
            oriPath = "file:///home/robert/Amadeus/data/ori_por_public.txt";//"hdfs:///user/rwaury/input/ori_por_public.csv";//
            regionPath = "file:///home/robert/Amadeus/data/ori_country_region_info.csv";//"hdfs:///user/rwaury/input/ori_country_region_info.csv";//
            defaultCapacityPath = "file:///home/robert/Amadeus/data/default_capacities.csv";//"hdfs:///user/rwaury/input/default_capacities.csv";//
            capacityPath = "file:///home/robert/Amadeus/data/capacities_2014-07-01.csv";//"hdfs:///user/rwaury/input/capacities_2014-07-01.csv";//
            mctPath = "file:///home/robert/Amadeus/data/mct.csv";//"hdfs:///user/rwaury/input/mct.csv";//
            outputPath = "file:///home/robert/Amadeus/data/resultConnections/";//"hdfs:///user/rwaury/output/flights/";//
            return true;
        }
        if(args.length == 1) {
            schedulePath = "hdfs:///user/rwaury/input/all_catalog_140410.txt";//
            oriPath = "hdfs:///user/rwaury/input/ori_por_public.csv";//
            regionPath = "hdfs:///user/rwaury/input/ori_country_region_info.csv";//
            defaultCapacityPath = "hdfs:///user/rwaury/input/default_capacities.csv";//
            capacityPath = "hdfs:///user/rwaury/input/capacities_2014-07-01.csv";//
            mctPath = "hdfs:///user/rwaury/input/mct.csv";//
            outputPath = "hdfs:///user/rwaury/output/flights/";//
            return true;
        }
        //System.err.println("Usage: FlightConnectionJoiner <schedule path> <ori path> <output path>");
        return false;
    }

}

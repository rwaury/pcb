package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * parse SSIM7 schedule in CSV format
 */
public class FilteringUTCExtractor implements FlatMapFunction<String, Flight> {

    private static final String DELIM = "\\^";
    private static final String HEADER = "#";

    private String[] tmp = null;

    private Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+0000"), Locale.FRENCH);

    private SimpleDateFormat format = new SimpleDateFormat("yyMMddHHmm");

    private Date earliest = new Date(ParallelConnectionBuilder.START);
    private Date latest = new Date(ParallelConnectionBuilder.END);

    private static final ArrayList<String> nonAirCraftList = new ArrayList<String>() {{
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
        tmp = value.split(DELIM);
        if (tmp[0].trim().startsWith(HEADER)) {
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
        }
        if((arrival.getTime() - departure.getTime()) <= 60L*1000L) {
            return; // flights of a minute or less are ignored
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
        Character trafficRestriction = CBUtil.EMPTY;
        if (!tmp[12].trim().isEmpty()) {
            trafficRestriction = tmp[12].trim().charAt(0);
        }
        Integer flightNumber = Integer.parseInt(tmp[14].trim());
        out.collect(new Flight(originAirport, originTerminal, "", "", "", "", 0.0, 0.0, departure.getTime(),
                destinationAirport, destinationTerminal, "", "", "", "", 0.0, 0.0, arrival.getTime(),
                airline, flightNumber, aircraft, -1, codeshareInfo, trafficRestriction));
    }
}
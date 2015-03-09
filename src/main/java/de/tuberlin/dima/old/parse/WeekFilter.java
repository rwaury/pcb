package de.tuberlin.dima.old.parse;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

@SuppressWarnings("serial")
public class WeekFilter implements FlatMapFunction<String, String> {

    public static final long START 	= 1398902400000L;//1398902400000L;
    public static final long END 	= 1401580800000L;//1399507200000L;//1399020800000L;//

    String[] tmp = null;

    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+0000"), Locale.FRENCH);

    SimpleDateFormat format = new SimpleDateFormat("yyMMddHHmm");

    Date earliest = new Date(START); // 1405040000
    Date latest = new Date(END); // 1405070000

    private final static ArrayList<String> nonAirCraftList = new ArrayList<String>() {{
        add("AGH");add("BH2");add("BUS");add("ICE");
        add("LCH");add("LMO");add("MD9");add("NDE");
        add("S61");add("S76");add("TRN");add("TSL");
    }};

    @Override
    public void flatMap(String value, Collector<String> out)
            throws Exception {
        tmp = value.split("\\^");
        if(tmp[0].trim().startsWith("#")) {
            // header
            return;
        }
        if(nonAirCraftList.contains(tmp[9].trim())) {
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
        // only flights departing between 1405010000 and 1405070000
        if(departure.before(earliest) || departure.after(latest)) {
            return;
        }
        if(tmp.length > 37 && tmp[37].trim().isEmpty() && !tmp[8].trim().equals(tmp[13].trim())) {
            // not an operating carrier
            // if airline and aircraft owner are identical the rule becomes moot
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
				/*
				throw new Exception("Sanity check failed! Arrival equal to or earlier than departure.\n" +
						"Departure: " + departure.toString() + " Arrival: " + arrival.toString() + "\n"  +
						"Sign: " + sign + " Hours: " + hours + " Minutes: " + minutes + "\n" +
						"Original value: " + value);*/
        }
        out.collect(value);
    }

}
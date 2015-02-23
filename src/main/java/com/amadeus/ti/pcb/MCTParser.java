package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * parses CSV MCT file
 */
public class MCTParser implements FlatMapFunction<String, MCTEntry> {

    private final static String DELIM = ",";
    private final static String HEADER = "#";
    private final static String DEFAULT = "***";

    private String[] tmp;

    @Override
    public void flatMap(String s, Collector<MCTEntry> out) throws Exception {
        if(s.startsWith(HEADER) || s.startsWith(DEFAULT)) {
            return;
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        tmp = s.split(DELIM);

        String arrival = tmp[0].trim();
        String stat = tmp[1].trim();
        String departure  = tmp[2].trim();
        String arrivalCarrier = tmp[3].trim();
        String departureCarrier = tmp[4].trim();
        String arrivalAircraft = tmp[5].trim();
        String departureAircraft = tmp[6].trim();
        String arrivalTerminal = tmp[7].trim();
        String departureTerminal = tmp[8].trim();
        String previousCountry = tmp[9].trim();
        String previousCity = tmp[10].trim();
        String previousAP = tmp[11].trim();
        String nextCountry = tmp[12].trim();
        String nextCity = tmp[13].trim();
        String nextAP = tmp[14].trim();

        String inFlightNoStr = tmp[15].trim();
        Integer inFlightNo = 0;
        if(!inFlightNoStr.isEmpty()) {
            inFlightNo = Integer.parseInt(inFlightNoStr);
        }

        String inFlightNoEORStr = tmp[16].trim();
        Integer inFlightNoEOR = 0;
        if(!inFlightNoEORStr.isEmpty()) {
            inFlightNoEOR = Integer.parseInt(inFlightNoEORStr);
        }

        String outFlightNoStr = tmp[17].trim();
        Integer outFlightNo = 0;
        if(!outFlightNoStr.isEmpty()) {
            outFlightNo = Integer.parseInt(outFlightNoStr);
        }

        String outFlightNoEORStr = tmp[18].trim();
        Integer outFlightNoEOR = 0;
        if(!outFlightNoEORStr.isEmpty()) {
            outFlightNoEOR = Integer.parseInt(outFlightNoEORStr);
        }

        String previousState = tmp[19].trim();
        String nextState = tmp[20].trim();
        String previousRegion = tmp[21].trim();
        String nextRegion = tmp[22].trim();

        String validFromStr = tmp[23].trim();
        Long validFrom = 0L;
        if(!validFromStr.isEmpty()) {
            Date fromDate =  format.parse(validFromStr);
            validFrom = fromDate.getTime();
        }

        String validUntilStr = tmp[24].trim();
        Long validUntil = Long.MAX_VALUE;
        if(!validUntilStr.isEmpty()) {
            Date untilDate =  format.parse(validUntilStr);
            validUntil = untilDate.getTime();
        }

        Integer mct = Integer.parseInt(tmp[25].trim());

        MCTEntry mctEntry = new MCTEntry(
                arrival, stat, departure, arrivalCarrier, departureCarrier, arrivalAircraft, departureAircraft,
                arrivalTerminal, departureTerminal, previousCountry, previousCity, previousAP, nextCountry, nextCity, nextAP,
                inFlightNo, inFlightNoEOR, outFlightNo, outFlightNoEOR,
                previousState, nextState, previousRegion, nextRegion, validFrom, validUntil, mct);
        out.collect(mctEntry);
    }
}

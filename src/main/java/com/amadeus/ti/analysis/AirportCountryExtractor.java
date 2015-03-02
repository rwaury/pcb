package com.amadeus.ti.analysis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

// extract airport -> [region, country, state] mapping (region is added later)
public class AirportCountryExtractor implements FlatMapFunction<String, Tuple4<String, String, String, String>> {

    String[] tmp = null;
    String from = null;
    String until = null;

    @Override
    public void flatMap(String value, Collector<Tuple4<String, String, String, String>> out) throws Exception {
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
        String iataCode = tmp[0].trim();
        String regionCode = "";
        String countryCode = tmp[16].trim();
        String stateCode = tmp[40].trim();
        out.collect(new Tuple4<String, String, String, String>(iataCode, regionCode, countryCode, stateCode));
    }
}
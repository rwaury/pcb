package de.tuberlin.dima.ti.pcb;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * extracts country-region mapping
 */
public class RegionExtractor implements MapFunction<String, Tuple2<String, String>> {

    private static final String DELIM = "\\^";

    private String[] tmp = null;

    @Override
    public Tuple2<String, String> map(String s) throws Exception {
        tmp = s.split(DELIM);
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
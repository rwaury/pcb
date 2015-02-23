package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * TODO: IMPLEMENT ME
 */
public class ThreeLegTrafficRestrictionsFilter implements FilterFunction<Tuple3<Flight, Flight, Flight>> {

    private final String exceptionsGeneral = "ABHIMTDEG";
    private final String exceptionsInternational = "NOQW";
    private final String exceptionsDomestic = "C";

    @Override
    public boolean filter(Tuple3<Flight, Flight, Flight> value) throws Exception {
        return true;
    }
}
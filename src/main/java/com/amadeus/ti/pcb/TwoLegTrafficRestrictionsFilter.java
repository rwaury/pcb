package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TwoLegTrafficRestrictionsFilter implements FilterFunction<Tuple2<Flight, Flight>> {

    private final String exceptionsGeneral = "ABHIMTDEG";
    private final String exceptionsInternational = "NOQW";
    private final String exceptionsDomestic = "C";

    @Override
    public boolean filter(Tuple2<Flight, Flight> value) throws Exception {
        if (exceptionsGeneral.indexOf(value.f0.getTrafficRestriction()) >= 0) {
            return false;
        } else if (!CBUtil.isDomestic(value.f1) && exceptionsDomestic.indexOf(value.f0.getTrafficRestriction()) >= 0) {
            return false;
        } else if (!CBUtil.isDomestic(value.f0) && exceptionsDomestic.indexOf(value.f1.getTrafficRestriction()) >= 0) {
            return false;
        } else if (CBUtil.isDomestic(value.f1) && exceptionsInternational.indexOf(value.f0.getTrafficRestriction()) >= 0) {
            return false;
        } else if (CBUtil.isDomestic(value.f0) && exceptionsInternational.indexOf(value.f1.getTrafficRestriction()) >= 0) {
            return false;
        } else {
            return true;
        }
    }
}
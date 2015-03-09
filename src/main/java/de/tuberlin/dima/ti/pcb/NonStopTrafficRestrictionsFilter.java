package de.tuberlin.dima.ti.pcb;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Discard all non-stop flights that can only be part of multi-leg flights
 */
public class NonStopTrafficRestrictionsFilter implements FilterFunction<Flight> {

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
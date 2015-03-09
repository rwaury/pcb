package de.tuberlin.dima.old.join;

import org.apache.flink.api.java.tuple.Tuple24;

// arrival,stat,departure,arr_carrier,dep_carrier,arr_aircraft,
// dep_aircraft,arr_terminal,dep_terminal,prev_country,prev_city,prev_airport,
// next_country,next_city,next_airport,in_flt_num,in_flt_eor,out_flt_num,
// out_flt_eor,prev_state,next_state,prev_region,next_region,(eff_date, dis_date /*dropped*/) ,mct
public class MCTEntry extends Tuple24<String, String, String, String, String, String,
        String, String, String, String, String, String,
        String, String, String, Integer, Integer, Integer,
        Integer, String, String, String, String, Integer>{

    public MCTEntry() {super();}


    public String getArrival() {
        return f0;
    }

    public String getStat() {
        return f1;
    }

    public String getDeparture() {
        return f2;
    }

    public String getArrCarrier() {
        return f3;
    }

    public String getDepCarrier() {
        return f4;
    }

    public String getArrAircraft() {
        return f5;
    }

    public String getDepAircraft() {
        return f6;
    }

    public String getArrTerminal() {
        return f7;
    }

    public String getDepTerminal() {
        return f8;
    }

    public String getPrevCountry() {
        return f9;
    }

    public String getPrevCity() {
        return f10;
    }

    public String getPrevAirport() {
        return f11;
    }

    public String getNextCountry() {
        return f12;
    }

    public String getNextCity() {
        return f13;
    }

    public String getNextAirport() {
        return f14;
    }

    public Integer getInFlightNumber() {
        return f15;
    }

    public Integer getInFlightEOR() {
        return f16;
    }

    public Integer getOutFlightNumber() {
        return f17;
    }

    public Integer getOutFlightEOR() {
        return f18;
    }

    public String getPrevState() {
        return f19;
    }

    public String getNextState() {
        return f20;
    }

    public String getPrevRegion() {
        return f21;
    }

    public String getNextRegion() {
        return f22;
    }

    public Integer getMCT() {
        return f23;
    }

}

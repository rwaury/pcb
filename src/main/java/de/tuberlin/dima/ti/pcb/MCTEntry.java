package de.tuberlin.dima.ti.pcb;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple25;

/**
 * contains all information of an MCT rule
 */
public class MCTEntry extends Tuple25<String, String, String, String, String, String,
        String, String, String, String, String, String,
        String, String, String, Integer, Integer, Integer,
        Integer, String, String, String, String, Tuple2<Long, Long>, Integer> {

    public MCTEntry() {
        super();
        this.f23 = new Tuple2<Long, Long>(0L, Long.MAX_VALUE);
    }

    public MCTEntry(String arrival, String stat, String departure,
                    String arrivalCarrier, String departureCarrier, String arrivalAircraft, String departureAircraft, String arrivalTerminal , String departureTerminal,
                    String previousCountry, String previousCity, String previousAP, String nextCountry, String nextCity, String nextAP,
                    Integer inFlightNo, Integer inFlightNoEOR, Integer outFlightNo, Integer outFlightNoEOR,
                    String previousState, String nextState, String previousRegion, String nextRegion,
                    Long validFrom, Long validUntil,
                    Integer mct) {
        this.f0 = arrival;
        this.f1 = stat;
        this.f2 = departure;
        this.f3 = arrivalCarrier;
        this.f4 = departureCarrier;
        this.f5 = arrivalAircraft;
        this.f6 = departureAircraft;
        this.f7 = arrivalTerminal;
        this.f8 = departureTerminal;
        this.f9 = previousCountry;
        this.f10 = previousCity;
        this.f11 = previousAP;
        this.f12 = nextCountry;
        this.f13 = nextCity;
        this.f14 = nextAP;
        this.f15 = inFlightNo;
        this.f16 = inFlightNoEOR;
        this.f17 = outFlightNo;
        this.f18 = outFlightNoEOR;
        this.f19 = previousState;
        this.f20 = nextState;
        this.f21 = previousRegion;
        this.f22 = nextRegion;
        this.f23 = new Tuple2<Long, Long>(validFrom, validUntil);
        this.f24 = mct;
    }

    public MCTEntry(Tuple25<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer, Integer, String, String, String, String, Tuple2<Long, Long>, Integer> copy) {
        this.f0 = copy.f0;
        this.f1 = copy.f1;
        this.f2 = copy.f2;
        this.f3 = copy.f3;
        this.f4 = copy.f4;
        this.f5 = copy.f5;
        this.f6 = copy.f6;
        this.f7 = copy.f7;
        this.f8 = copy.f8;
        this.f9 = copy.f9;
        this.f10 = copy.f10;
        this.f11 = copy.f11;
        this.f12 = copy.f12;
        this.f13 = copy.f13;
        this.f14 = copy.f14;
        this.f15 = copy.f15;
        this.f16 = copy.f16;
        this.f17 = copy.f17;
        this.f18 = copy.f18;
        this.f19 = copy.f19;
        this.f20 = copy.f20;
        this.f21 = copy.f21;
        this.f22 = copy.f22;
        this.f23 = new Tuple2<Long, Long>(copy.f23.f0, copy.f23.f1);
        this.f24 = copy.f24;
    }


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

    public Long getValidFrom() {
        return f23.f0;
    }

    public Long getValidUntil() {
        return f23.f1;
    }

    public Integer getMCT() {
        return f24;
    }

    public MCTEntry deepCopy() {
        return new MCTEntry(this.copy());
    }

}

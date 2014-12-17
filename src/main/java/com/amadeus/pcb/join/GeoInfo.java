package com.amadeus.pcb.join;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class GeoInfo extends Tuple8<String, String, String, String, String, String, Double, Double> {

    public static final String DELIM = "^";

    public GeoInfo() {super();}

    public GeoInfo(String airport, String terminal, String city, String state, String country, String region, Double latitude, Double longitude) {
        this.f0 = airport;
        this.f1 = terminal;
        this.f2 = city;
        this.f3 = state;
        this.f4 = country;
        this.f5 = region;
        this.f6 = latitude;
        this.f7 = longitude;
    }

    private GeoInfo(Tuple8<String, String, String, String, String, String, Double, Double> info) {
        this.f0 = info.f0;
        this.f1 = info.f1;
        this.f2 = info.f2;
        this.f3 = info.f3;
        this.f4 = info.f4;
        this.f5 = info.f5;
        this.f6 = info.f6;
        this.f7 = info.f7;
    }

    public GeoInfo clone() {
        return new GeoInfo(this.copy());
    }

    public void write(DataOutputView out) throws IOException {
        out.writeUTF(this.f0);
        out.writeUTF(this.f1);
        out.writeUTF(this.f2);
        out.writeUTF(this.f3);
        out.writeUTF(this.f4);
        out.writeUTF(this.f5);
        out.writeDouble(this.f6);
        out.writeDouble(this.f7);
    }

    public void read(DataInputView in) throws IOException {
        this.f0 = in.readUTF();
        this.f1 = in.readUTF();
        this.f2 = in.readUTF();
        this.f3 = in.readUTF();
        this.f4 = in.readUTF();
        this.f5 = in.readUTF();
        this.f6 = in.readDouble();
        this.f7 = in.readDouble();
    }

    @Override
    public String toString() {
        return this.f0;
    }
}
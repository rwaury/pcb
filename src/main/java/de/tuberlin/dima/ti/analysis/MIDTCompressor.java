package de.tuberlin.dima.ti.analysis;

import org.apache.flink.api.common.functions.MapFunction;

// merge multi-leg flights in MIDT to be comparable to CB result
public class MIDTCompressor implements MapFunction<MIDT, MIDT> {

    @Override
    public MIDT map(MIDT midt) throws Exception {
        String flight1 = midt.f3;
        String flight2 = midt.f4;
        String flight3 = midt.f5;
        String flight4 = midt.f6;
        String flight5 = midt.f7;
        if(flight1.equals(flight2)) {
            flight2 = flight3;
            flight3 = flight4;
            flight4 = flight5;
            flight5 = "";
        }
        if(flight2.equals(flight3)) {
            flight3 = flight4;
            flight4 = flight5;
            flight5 = "";
        }
        if(flight3.equals(flight4)) {
            flight4 = flight5;
            flight5 = "";
        }
        if(flight4.equals(flight5)) {
            flight5 = "";
        }
        return new MIDT(midt.f0, midt.f1, midt.f2, flight1, flight2, flight3, flight4, flight5, midt.f8, midt.f9, midt.f10, midt.f11, midt.f12, midt.f13);
    }
}
package com.amadeus.ti.analysis;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

// merges upper (CB OD capacity) and lower bound (MIDT hits) of an OD in one tuple
public class ODBoundMerger implements CoGroupFunction<Tuple5<String, String, String, Integer, Integer>,
        Tuple5<String, String, String, Integer, Integer>, Tuple5<String, String, String, Integer, Integer>> {

    @Override
    public void coGroup(Iterable<Tuple5<String, String, String, Integer, Integer>> lower,
                        Iterable<Tuple5<String, String, String, Integer, Integer>> upper,
                        Collector<Tuple5<String, String, String, Integer, Integer>> out) throws Exception {
        Tuple5<String, String, String, Integer, Integer> result = new Tuple5<String, String, String, Integer, Integer>();
        for (Tuple5<String, String, String, Integer, Integer> l : lower) {
            result.f0 = l.f0;
            result.f1 = l.f1;
            result.f2 = l.f2;
            result.f3 = l.f3;
            result.f4 = l.f4;
        }
        for (Tuple5<String, String, String, Integer, Integer> u : upper) {
            result.f0 = u.f0;
            result.f1 = u.f1;
            result.f2 = u.f2;
            if (result.f3 == null) {
                result.f3 = 0;
            }
            result.f4 = u.f4;
        }
        out.collect(result);
    }
}
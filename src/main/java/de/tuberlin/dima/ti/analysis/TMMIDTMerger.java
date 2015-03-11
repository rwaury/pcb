package de.tuberlin.dima.ti.analysis;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.Iterator;

// since the MIDT lower bound was removed before the IPF run we add it back up
public class TMMIDTMerger implements CoGroupFunction<Tuple5<String, String, String, Double, SerializableVector>,
        Tuple5<String, String, String, Integer, Integer>,
        Tuple5<String, String, String, Double, SerializableVector>> {

    @Override
    public void coGroup(Iterable<Tuple5<String, String, String, Double, SerializableVector>> tm,
                        Iterable<Tuple5<String, String, String, Integer, Integer>> midtBound,
                        Collector<Tuple5<String, String, String, Double, SerializableVector>> out) throws Exception {
        Iterator<Tuple5<String, String, String, Integer, Integer>> midtIter = midtBound.iterator();
        Iterator<Tuple5<String, String, String, Double, SerializableVector>> tmIter = tm.iterator();
        if (!midtIter.hasNext()) {
            out.collect(tmIter.next());
        } else {
            Tuple5<String, String, String, Integer, Integer> midt = midtIter.next();
            if (tmIter.hasNext()) {
                Tuple5<String, String, String, Double, SerializableVector> tmEntry = tmIter.next();
                out.collect(new Tuple5<String, String, String, Double, SerializableVector>
                        (tmEntry.f0, tmEntry.f1, tmEntry.f2, Math.min(tmEntry.f3 + (double) midt.f3, (double) midt.f4), tmEntry.f4));
            }
        }
        if (tmIter.hasNext()) {
            throw new Exception("More than one TM entry: " + tmIter.next().toString());
        }
        if (midtIter.hasNext()) {
            throw new Exception("More than one MIDT bound: " + midtIter.next().toString());
        }
    }
}

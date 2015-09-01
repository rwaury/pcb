package de.tuberlin.dima.ti.analysis;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

// merge MIDT data from different GDS systems and booking locations
public class MIDTGrouper extends RichGroupReduceFunction<MIDT, MIDT> {

    @Override
    public void reduce(Iterable<MIDT> midts, Collector<MIDT> out) throws Exception {
        int paxSum = 0;
        int count = 0;
        Iterator<MIDT> iterator = midts.iterator();
        MIDT midt = null;
        while(iterator.hasNext()) {
            midt = iterator.next();
            paxSum += midt.f11;
            count++;
        }
        MIDT result = new MIDT(midt.f0, midt.f1, midt.f2, midt.f3, midt.f4, midt.f5, midt.f6, midt.f7, midt.f8, midt.f9, midt.f10, paxSum, midt.f12, midt.f13, midt.f14, midt.f15);
        out.collect(result);
    }
}
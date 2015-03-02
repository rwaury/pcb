package com.amadeus.ti.analysis;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

// assign weights to ODs without training data (very slow at the moment, performs day wise NL-join)
public class ODDistanceComparator implements
        CoGroupFunction<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>,
                Tuple5<String, String, String, Double, SerializableVector>,
                Tuple5<String, String, String, Double, LogitOptimizable>> {

    //private List<Tuple2<String, SerializableMatrix>> matrices = null;

        /*@Override
        public void open(Configuration parameters) {
            //this.matrices = getRuntimeContext().getBroadcastVariable(INVERTED_COVARIANCE_MATRIX);
        }*/

    @Override
    public void coGroup(Iterable<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> weightedODs,
                        Iterable<Tuple5<String, String, String, Double, SerializableVector>> unweightedODs,
                        Collector<Tuple5<String, String, String, Double, LogitOptimizable>> out) throws Exception {

        ArrayList<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> weighted = new ArrayList<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>>();
        for(Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> w : weightedODs) {
            weighted.add(w.copy());
        }
        double distance = 0.0;
        for(Tuple5<String, String, String, Double, SerializableVector> uw : unweightedODs) {
            double minDistance = Double.MAX_VALUE;
            Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> tmp = null;
            for(Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> w : weighted) {
                distance = uw.f4.getVector().getDistance(w.f4.getVector());
                if(distance < minDistance) {
                    minDistance = distance;
                    tmp = w;
                }
            }
            if(tmp == null) {
                continue;
            }
            out.collect(new Tuple5<String, String, String, Double, LogitOptimizable>(uw.f0, uw.f1, uw.f2, uw.f3, tmp.f5));
        }
    }
}
package de.tuberlin.dima.ti.analysis;

import org.apache.commons.math3.exception.NumberIsTooSmallException;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.stat.correlation.StorelessCovariance;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

import static org.apache.commons.math3.linear.MatrixUtils.inverse;

// assign weights to ODs without training data (very slow at the moment, performs day wise NL-join)
public class ODDistanceComparator implements
        CoGroupFunction<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>,
                Tuple5<String, String, String, Double, SerializableVector>,
                Tuple5<String, String, String, Double, LogitOptimizable>> {

    private static final boolean BIAS_CORRECTION = true;

    private boolean useEuclidean;

    public ODDistanceComparator(boolean useEuclidean) {
        this.useEuclidean = useEuclidean;
    }

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
        if(useEuclidean) {
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
        } else {
            ArrayRealVector mu = new ArrayRealVector(TrafficAnalysis.OD_FEATURE_COUNT, 0.0);
            int count = 0;
            StorelessCovariance S = new StorelessCovariance(TrafficAnalysis.OD_FEATURE_COUNT, BIAS_CORRECTION);
            for(Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> w : weightedODs) {
                mu.add(w.f4.getVector());
                weighted.add(w.copy());
                S.increment(w.f4.getVector().toArray());
                count++;
            }
            mu.mapDivideToSelf((double) count);
            RealMatrix Sinv;
            try {
                Sinv = inverse(S.getCovarianceMatrix());
            } catch(NumberIsTooSmallException e) {
                throw new Exception(e.getMessage() + "count: " + count);
            }
            double distance = 0.0;
            for(Tuple5<String, String, String, Double, SerializableVector> uw : unweightedODs) {
                double minDistance = Double.MAX_VALUE;
                Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> tmp = null;
                for(Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> w : weighted) {
                    distance = mahalanobisDistance(uw.f4.getVector(), w.f4.getVector(), mu, Sinv);
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

    private double mahalanobisDistance(ArrayRealVector x, ArrayRealVector y, ArrayRealVector mu, RealMatrix Sinv) {
        ArrayRealVector xmmu = x.subtract(mu);
        ArrayRealVector ymmu = y.subtract(mu);
        double result = Sinv.preMultiply(xmmu).dotProduct(ymmu);
        return Math.sqrt(result);
    }
}
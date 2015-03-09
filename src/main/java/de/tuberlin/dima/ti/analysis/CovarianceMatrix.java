package de.tuberlin.dima.ti.analysis;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

// TODO: implement covariance matrix of OD feature vectors to compute Mahalanobis distance between ODs and not Euclidean
public class CovarianceMatrix implements GroupReduceFunction<Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>,
        Tuple2<String, SerializableMatrix>> {

    @Override
    public void reduce(Iterable<Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>> iterable,
                       Collector<Tuple2<String, SerializableMatrix>> collector) throws Exception {

    }
}
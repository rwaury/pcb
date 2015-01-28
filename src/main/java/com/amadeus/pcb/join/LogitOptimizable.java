package com.amadeus.pcb.join;

import cc.mallet.optimize.Optimizable;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;

public class LogitOptimizable extends Tuple4<Double, Double, Double, Double> implements Optimizable.ByGradientValue {

    private static int FEATURE_COUNT = 3;

    private ArrayList<TrainingData> trainingData;

    private double[] valueCache = null;

    public LogitOptimizable() {
        super();
        for(int i = 0; i < this.getArity(); i++) {
            this.setField(1.0, i);
        }
    }

    public void setTrainingData(ArrayList<TrainingData> trainingData, int minTravelTime) {
        this.trainingData = trainingData;
        for(TrainingData t : this.trainingData) {
            t.features[1] = t.features[1]/(double)minTravelTime;
        }
        this.valueCache = new double[this.getArity()];
        for(int i = 0; i < this.getArity(); i++) {
            valueCache[i] = this.getField(i);
        }
    }

    public void clear() {
        this.trainingData.clear();
        for(int i = 0; i < this.valueCache.length; i++) {
            this.setField(this.valueCache[i], i);
        }
        this.valueCache = null;
    }

    public double[] asArray() {
        double[] weights = new double[this.getArity()];
        for(int i = 0; i < this.getArity(); i++) {
            weights[i] = this.getField(i);
        }
        return weights;
    }

    @Override
    public void getValueGradient(double[] gradient) {
        assert  gradient.length == this.getArity();
        double[] avg = new double[gradient.length];
        for(int i = 0; i < this.trainingData.size(); i++) {
            TrainingData ti = this.trainingData.get(i);
            double[] tmp = new double[gradient.length];
            add(tmp, ti.features);
            multiply(tmp, softmax(i));
            add(avg, tmp);
        }
        for(int i = 0; i < gradient.length; i++) {
            gradient[i] = 0.0;
        }
        for(int i = 0; i < this.trainingData.size(); i++) {
            TrainingData ti = this.trainingData.get(i);
            double[] s = minus(ti.features, avg);
            multiply(s, (double)ti.hits);
            add(gradient, s);
        }
    }

    @Override
    public double getValue() {
        double sum = 0.0;
        for(int i = 0; i < this.trainingData.size(); i++) {
            TrainingData t = this.trainingData.get(i);
            sum += t.hits * Math.log(softmax(i));
        }
        return sum;
    }

    @Override
    public int getNumParameters() {
        return this.getArity();
    }

    @Override
    public void getParameters(double[] doubles) {
        assert doubles.length == this.valueCache.length;
        for(int i = 0; i < this.valueCache.length; i++) {
            doubles[i] = this.valueCache[i];
        }
    }

    @Override
    public double getParameter(int i) {
        return this.valueCache[i];
    }

    @Override
    public void setParameters(double[] doubles) {
        assert doubles.length == this.valueCache.length;
        for(int i = 0; i < this.valueCache.length; i++) {
            this.valueCache[i] = doubles[i];
        }
    }

    @Override
    public void setParameter(int i, double v) {
        this.valueCache[i] = v;
    }

    private double softmax(int i) {
        double sum = 0.0;
        for(TrainingData t : this.trainingData) {
            sum += Math.exp(linearPredictorFunction(t.features));
        }
        double result = Math.exp(linearPredictorFunction(this.trainingData.get(i).features))/sum;
        return result;
    }

    private double linearPredictorFunction(double[] itinerary) {
        assert itinerary.length == valueCache.length;
        double sum = 0.0;
        for(int i = 0; i < this.valueCache.length; i++) {
            sum += itinerary[i]*this.valueCache[i];
        }
        return sum;
    }

    private void add(double[] a, double[] b) {
        assert a.length == b.length;
        for (int i = 0; i < a.length; i++) {
            a[i] = a[i] + b[i];
        }
    }

    private double[] minus(double[] a, double[] b) {
        assert a.length == b.length;
        double[] difference = new double[a.length];
        for (int i = 0; i < a.length; i++) {
            difference[i] = a[i] - b[i];
        }
        return difference;
    }

    private void multiply(double[] v, double s) {
        for (int i = 0; i < v.length; i++) {
            v[i] *= s;
        }
    }

    public static double[] toArray(Itinerary iter, int minTime) {
        if(minTime < 1) {
            minTime = 1;
        }
        double[] features = new double[FEATURE_COUNT+1];
        features[0] = 1.0; // w0 + w1x1 + ...
        features[1] = iter.f9/minTime;
        features[2] = iter.f10/iter.f9;
        features[3] = (double) iter.f12;
        return features;
    }

    public static double softmax(Itinerary iter, double softmaxSum, double[] weights, int minTime) {
        return Math.exp(linearPredictorFunction(toArray(iter, minTime), weights))/softmaxSum;
    }

    public static double linearPredictorFunction(double[] itinerary, double[] weights) {
        assert itinerary.length == weights.length;
        double sum = 0.0;
        for(int i = 0; i < weights.length; i++) {
            sum += itinerary[i]*weights[i];
        }
        return sum;
    }

    public static class TrainingData {

        public double[] features;
        public int hits;

        public TrainingData(int travelTime, double percentageWaiting, int legCount, int hits) {
            this.features = new double[FEATURE_COUNT+1];
            this.features[0] = 1.0; // w0 + w1x1 + ...
            this.features[1] = (double) travelTime;
            this.features[2] = percentageWaiting;
            this.features[3] = (double) legCount;
            this.hits = hits;
        }

    }
}

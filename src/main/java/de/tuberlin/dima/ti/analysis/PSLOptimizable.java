package de.tuberlin.dima.ti.analysis;

import cc.mallet.optimize.Optimizable;

import java.util.ArrayList;

public class PSLOptimizable extends SerializableVector implements Optimizable.ByGradientValue {

    private static int FEATURE_COUNT = 6;

    private ArrayList<TrainingData> trainingData;

    private ArrayList<Double> PS;

    private double BETA;

    private double[] valueCache = null;

    public PSLOptimizable() {
        super(FEATURE_COUNT+1);
        for(int i = 0; i < this.getVector().getDimension(); i++) {
            this.getVector().setEntry(i, 1.0);
        }
        this.BETA = 1.0;
    }

    public PSLOptimizable(double beta) {
        super(FEATURE_COUNT+1);
        this.BETA = beta;
        for(int i = 0; i < this.getVector().getDimension(); i++) {
            this.getVector().setEntry(i, 1.0);
        }
    }

    public void setTrainingData(ArrayList<TrainingData> trainingData, int minTravelTime, ArrayList<Double> PS) {
        this.trainingData = trainingData;
        this.PS = PS;
        assert PS.size() == trainingData.size();
        for(TrainingData t : this.trainingData) {
            t.features[1] = t.features[1]/(double)minTravelTime;
        }
        this.valueCache = new double[this.getVector().getDimension()];
        for(int i = 0; i < this.getVector().getDimension(); i++) {
            valueCache[i] = this.getVector().getEntry(i);
        }
    }

    public void clear() {
        this.trainingData.clear();
        this.PS.clear();
        for(int i = 0; i < this.valueCache.length; i++) {
            this.getVector().setEntry(i, this.valueCache[i]);
        }
        this.valueCache = null;
    }

    public double[] asArray() {
        return this.getVector().toArray();
    }

    @Override
    public void getValueGradient(double[] gradient) {
        assert  gradient.length == this.getVector().getDimension();
        double[] sum = new double[gradient.length];
        for(int i = 0; i < this.trainingData.size(); i++) {
            TrainingData ti = this.trainingData.get(i);
            double[] tmp = new double[gradient.length];
            add(tmp, ti.features);
            multiply(tmp, softmax(i));
            add(sum, tmp);
            //add(avg, ti.features);
        }
        //double n = (double) this.trainingData.size();
        //multiply(avg, 1.0/n);
        for(int i = 0; i < gradient.length; i++) {
            gradient[i] = 0.0;
        }
        for(int i = 0; i < this.trainingData.size(); i++) {
            TrainingData ti = this.trainingData.get(i);
            double[] s = minus(ti.features, sum);
            multiply(s, (double)ti.hits);
            add(gradient, s);
        }
        multiply(gradient, -1.0);
    }

    @Override
    public double getValue() {
        double sum = 0.0;
        for(int i = 0; i < this.trainingData.size(); i++) {
            TrainingData t = this.trainingData.get(i);
            sum += t.hits * Math.log(softmax(i));
        }
        //return -sum;
        return sum;
    }

    @Override
    public int getNumParameters() {
        return FEATURE_COUNT+1;
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
        for(int j = 0; j < this.trainingData.size(); j++) {
            TrainingData t = this.trainingData.get(j);
            double ps = this.PS.get(j);
            sum += Math.exp(linearPredictorFunction(t.features, ps));
        }
        double result = Math.exp(linearPredictorFunction(this.trainingData.get(i).features, this.PS.get(i)))/sum;
        return result;
    }

    private double linearPredictorFunction(double[] itinerary , double ps) {
        assert itinerary.length == valueCache.length;
        double sum = 0.0;
        for(int i = 0; i < this.valueCache.length; i++) {
            sum += itinerary[i]*this.valueCache[i];
        }
        return sum + (BETA*ps);
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

    public static double[] toArray(Itinerary itinerary, int minTime) {
        if(minTime < 1) {
            minTime = 1;
        }
        double[] features = new double[FEATURE_COUNT+1];
        features[0] = 1.0; // w0 + w1x1 + ...
        features[1] = itinerary.f10/minTime;
        features[2] = itinerary.f11/itinerary.f10;
        features[3] = (double) itinerary.f12;
        features[4] = itinerary.getGeoDetour();
        features[5] = (double)itinerary.f19;
        features[6] = (double)itinerary.getNumAirLines();
        return features;
    }

    public static double softmaxPSCF(Itinerary iter, double softmaxSum, double[] weights, int minTime, double ps, double beta) {
        return Math.exp(linearPredictorFunction(toArray(iter, minTime), weights, ps, beta))/softmaxSum;
    }

    public static double linearPredictorFunction(double[] itinerary, double[] weights, double ps, double beta) {
        assert itinerary.length == weights.length;
        double sum = 0.0;
        for(int i = 0; i < weights.length; i++) {
            sum += itinerary[i]*weights[i];
        }
        return sum + (beta*ps);
    }

    public static class TrainingData {

        public double[] features;
        public int hits;

        public TrainingData(int travelTime, double percentageWaiting, int legCount, double geoDetour, int numCountries, int numAirlines, int hits) {
            this.features = new double[FEATURE_COUNT+1];
            this.features[0] = 1.0; // w0 + w1x1 + ...
            this.features[1] = (double) travelTime;
            this.features[2] = percentageWaiting;
            this.features[3] = (double) legCount;
            this.features[4] = geoDetour;
            this.features[5] = (double) numCountries;
            this.features[6] = (double) numAirlines;
            this.hits = hits;
        }

    }
}

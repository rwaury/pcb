package com.amadeus.pcb.util;

import cc.mallet.optimize.LimitedMemoryBFGS;
import cc.mallet.optimize.Optimizable;

import java.util.ArrayList;

public class LBFGS {

    public static void main(String[] args) {
        ArrayList<TrainingData> trainingData = new ArrayList<TrainingData>();
        trainingData.add(new TrainingData(1.0, 1.0, 1, 0.0, 70));
        trainingData.add(new TrainingData(1.5, 1.4, 2, 0.2, 20));
        trainingData.add(new TrainingData(1.2, 1.1, 2, 0.1, 15));
        trainingData.add(new TrainingData(1.6, 2.0, 3, 0.3, 2));

        ItineraryParameters optimizable = new ItineraryParameters(trainingData);
        LimitedMemoryBFGS optimizer = new LimitedMemoryBFGS(optimizable);

        optimizer.setTolerance(0.00001);

        boolean converged = false;
        try {
            converged = optimizer.optimize(1000);
        } catch (IllegalArgumentException e) {
            // This exception may be thrown if L-BFGS
            //  cannot step in the current direction.
            // This condition does not necessarily mean that
            //  the optimizer has failed, but it doesn't want
            //  to claim to have succeeded...
        } catch (Throwable t) {
            t.printStackTrace();
            System.out.println(optimizable.getParameter(0) + "," +
                    optimizable.getParameter(1) + "," +
                    optimizable.getParameter(2) + "," +
                    optimizable.getParameter(3) + "," +
                    optimizable.getParameter(4));
        }

        System.out.println(
                optimizable.getParameter(0) + "," +
                optimizable.getParameter(1) + "," +
                optimizable.getParameter(2) + "," +
                optimizable.getParameter(3) + "," +
                optimizable.getParameter(4));

        for(int i = 0; i < trainingData.size(); i++) {
            System.out.println(trainingData.get(i).hits + ": " + optimizable.softmax(i));
        }

        double[] weights = new double[optimizable.getNumParameters()];
        optimizable.getParameters(weights);

        ArrayList<Itinerary> itineraries = new ArrayList<Itinerary>();
        itineraries.add(new Itinerary(1.0, 1.0, 1, 0.0));
        itineraries.add(new Itinerary(1.1, 1.1, 2, 0.1));
        itineraries.add(new Itinerary(1.5, 1.4, 2, 0.2));
        itineraries.add(new Itinerary(1.2, 1.1, 2, 0.1));
        itineraries.add(new Itinerary(1.6, 2.0, 3, 0.3));
        itineraries.add(new Itinerary(1.8, 3.0, 3, 0.4));

        for(Itinerary iter : itineraries) {
            System.out.println(softmax(iter, itineraries, weights));
        }
    }

    private static class ItineraryParameters implements Optimizable.ByGradientValue {

        private static int PARAMETER_COUNT = TrainingData.FEATURE_COUNT + 1;

        double[] weights;
        ArrayList<TrainingData> trainingData;

        int k;

        public ItineraryParameters(ArrayList<TrainingData> trainingData) {
            this.k = trainingData.size();
            this.weights = new double[PARAMETER_COUNT];
            for(int i = 0; i < PARAMETER_COUNT; i++) {
                weights[i] = Math.random();
            }
            this.trainingData = trainingData;
        }

        public double getValue() {
            double sum = 0.0;
            for(int i = 0; i < k; i++) {
                TrainingData t = this.trainingData.get(i);
                sum += t.hits * Math.log(softmax(i));
            }
            return sum;
        }

        public void getValueGradient(double[] gradient) {
            assert  gradient.length == PARAMETER_COUNT;
            double[] avg = new double[gradient.length];
            for(int i = 0; i < k; i++) {
                TrainingData ti = this.trainingData.get(i);
                double[] tmp = new double[gradient.length];
                add(tmp, ti.features);
                multiply(tmp, softmax(i));
                add(avg, tmp);
            }
            for(int i = 0; i < gradient.length; i++) {
                gradient[i] = 0.0;
            }
            for(int i = 0; i < k; i++) {
                TrainingData ti = this.trainingData.get(i);
                double[] s = minus(ti.features, avg);
                multiply(s, (double)ti.hits);
                add(gradient, s);
            }
            //multiply(gradient, -1.0);
        }

        private double linearPredictorFunction(double[] itinerary) {
            return weights[0]*itinerary[0] + weights[1]*itinerary[1] + weights[2]*itinerary[2] + weights[3]*itinerary[3] + weights[4]*itinerary[4];
        }

        public double softmax(int i) {
            double sum = 0.0;
            for(TrainingData t : this.trainingData) {
                sum += Math.exp(linearPredictorFunction(t.features));
            }
            double result = Math.exp(linearPredictorFunction(this.trainingData.get(i).features))/sum;
            return result;
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

        public int getNumParameters() {
            return PARAMETER_COUNT;
        }

        public double getParameter(int i) {
            return weights[i];
        }

        public void getParameters(double[] buffer) {
            for(int i = 0; i < buffer.length; i++) {
                buffer[i] = weights[i];
            }
        }

        public void setParameter(int i, double r) {
            weights[i] = r;
        }

        public void setParameters(double[] newParameters) {
            for(int i = 0; i < newParameters.length; i++) {
                weights[i] = newParameters[i];
            }
        }
    }

    private static class TrainingData {

        private static int FEATURE_COUNT = 4;

        public double[] features;
        public int hits;


        public TrainingData(double detour, double compareToShortest, int legCount, double percentageWaiting, int hits) {
            this.features = new double[FEATURE_COUNT+1];
            this.features[0] = 1.0; // w0 + w1x1 + ...
            this.features[1] = detour;
            this.features[2] = compareToShortest;
            this.features[3] = (double) legCount;
            this.features[4] = percentageWaiting;
            this.hits = hits;
        }

    }

    private static class Itinerary {
        private static int FEATURE_COUNT = 4;

        public double[] features;

        public Itinerary(double detour, double compareToShortest, int legCount, double percentageWaiting) {
            this.features = new double[FEATURE_COUNT+1];
            this.features[0] = 1.0; // w0 + w1x1 + ...
            this.features[1] = detour;
            this.features[2] = compareToShortest;
            this.features[3] = (double) legCount;
            this.features[4] = percentageWaiting;
        }
    }

    public static double softmax(Itinerary iter, ArrayList<Itinerary> iterList, double[] weights) {
        double sum = 0.0;
        for(Itinerary it : iterList) {
            sum += Math.exp(linearPredictorFunction(it.features, weights));
        }
        return Math.exp(linearPredictorFunction(iter.features, weights))/sum;
    }

    private static double linearPredictorFunction(double[] itinerary, double[] weights) {
        return weights[0]*itinerary[0] + weights[1]*itinerary[1] + weights[2]*itinerary[2] + weights[3]*itinerary[3] + weights[4]*itinerary[4];
    }

}

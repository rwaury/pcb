package de.tuberlin.dima.old.join;

import org.apache.flink.api.java.tuple.*;

public class ItineraryInfo {

    public static class ItineraryInfo1 extends
            Tuple8<GeoInfo, GeoInfo, String, Integer, Double, Integer, Integer, Integer> {

        public ItineraryInfo1() {
            super();
            this.f0 = new GeoInfo();
            this.f4 = 0.0;
            this.f5 = 0;
            this.f6 = 0;
            this.f7 = 0;
        }

        public ItineraryInfo1(GeoInfo depInfo, GeoInfo arrInfo, String airline,
                              int capacity, double capacityShare, int minTime, int maxTime, int avgTime) {
            this.f0 = depInfo;
            this.f1 = arrInfo;
            this.f2 = airline;
            this.f3 = capacity;
            this.f4 = capacityShare;
            this.f5 = minTime;
            this.f6 = maxTime;
            this.f7 = avgTime;
        }

        public ItineraryInfo1(Tuple8<GeoInfo, GeoInfo, String, Integer, Double, Integer, Integer, Integer> tuple) {
            this.f0 = tuple.f0.clone();
            this.f1 = tuple.f1.clone();
            this.f2 = tuple.f2;
            this.f3 = tuple.f3;
            this.f4 = tuple.f4;
            this.f5 = tuple.f5;
            this.f6 = tuple.f6;
            this.f7 = tuple.f7;
        }

        public ItineraryInfo1 clone() {
            return new ItineraryInfo1(this.copy());
        }

    }

    public static class ItineraryInfo2 extends
            Tuple11<GeoInfo, GeoInfo, String, GeoInfo, GeoInfo, String, Integer, Double,  Integer, Integer, Integer> {

        public ItineraryInfo2() {
            super();
            this.f0 = new GeoInfo();
            this.f1 = new GeoInfo();
            this.f3 = new GeoInfo();
            this.f4 = new GeoInfo();
            this.f7 = 0.0;
            this.f8 = 0;
            this.f9 = 0;
            this.f10 = 0;
        }

        public ItineraryInfo2(GeoInfo depInfo1, GeoInfo arrInfo1, String airline1,
                              GeoInfo depInfo2, GeoInfo arrInfo2, String airline2,
                              int capacity, double capacityShare, int minTime, int maxTime, int avgTime) {
            this.f0 = depInfo1;
            this.f1 = arrInfo1;
            this.f2 = airline1;
            this.f3 = depInfo2;
            this.f4 = arrInfo2;
            this.f5 = airline2;
            this.f6 = capacity;
            this.f7 = capacityShare;
            this.f8 = minTime;
            this.f9 = maxTime;
            this.f10 = avgTime;
        }

        public ItineraryInfo2(Tuple11<GeoInfo, GeoInfo, String, GeoInfo, GeoInfo, String, Integer, Double, Integer, Integer, Integer> tuple) {
            this.f0 = tuple.f0.clone();
            this.f1 = tuple.f1.clone();
            this.f2 = tuple.f2;
            this.f3 = tuple.f3.clone();
            this.f4 = tuple.f4.clone();
            this.f5 = tuple.f5;
            this.f6 = tuple.f6;
            this.f7 = tuple.f7;
            this.f8 = tuple.f8;
            this.f9 = tuple.f9;
            this.f10 = tuple.f10;
        }

        public ItineraryInfo2 clone() {
            return new ItineraryInfo2(this.copy());
        }

    }

    public static class ItineraryInfo3 extends
            Tuple14<GeoInfo, GeoInfo, String, GeoInfo, GeoInfo, String, GeoInfo, GeoInfo, String, Integer, Double, Integer, Integer, Integer> {

        public ItineraryInfo3() {
            super();
            this.f0 = new GeoInfo();
            this.f1 = new GeoInfo();
            this.f3 = new GeoInfo();
            this.f4 = new GeoInfo();
            this.f6 = new GeoInfo();
            this.f7 = new GeoInfo();
            this.f10 = 0.0;
            this.f11 = 0;
            this.f12 = 0;
            this.f13 = 0;
        }

        public ItineraryInfo3(GeoInfo depInfo1, GeoInfo arrInfo1, String airline1,
                              GeoInfo depInfo2, GeoInfo arrInfo2, String airline2,
                              GeoInfo depInfo3, GeoInfo arrInfo3, String airline3,
                              int capacity, double capacityShare, int minTime, int maxTime, int avgTime) {
            this.f0 = depInfo1;
            this.f1 = arrInfo1;
            this.f2 = airline1;
            this.f3 = depInfo2;
            this.f4 = arrInfo2;
            this.f5 = airline2;
            this.f6 = depInfo3;
            this.f7 = arrInfo3;
            this.f8 = airline3;
            this.f9 = capacity;
            this.f10 = capacityShare;
            this.f11 = minTime;
            this.f12 = maxTime;
            this.f13 = avgTime;
        }

        public ItineraryInfo3(Tuple14<GeoInfo, GeoInfo, String, GeoInfo, GeoInfo, String, GeoInfo, GeoInfo, String, Integer, Double, Integer, Integer, Integer> tuple) {
            this.f0 = tuple.f0.clone();
            this.f1 = tuple.f1.clone();
            this.f2 = tuple.f2;
            this.f3 = tuple.f3.clone();
            this.f4 = tuple.f4.clone();
            this.f5 = tuple.f5;
            this.f6 = tuple.f6.clone();
            this.f7 = tuple.f7.clone();
            this.f8 = tuple.f8;
            this.f9 = tuple.f9;
            this.f10 = tuple.f10;
            this.f11 = tuple.f11;
            this.f12 = tuple.f12;
            this.f13 = tuple.f13;
        }

        public ItineraryInfo3 clone() {
            return new ItineraryInfo3(this.copy());
        }

    }
}

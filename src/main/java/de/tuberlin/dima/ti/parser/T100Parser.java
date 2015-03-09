package de.tuberlin.dima.ti.parser;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class T100Parser {

    public static class MarketParser implements FlatMapFunction<String, T100Market> {

        @Override
        public void flatMap(String s, Collector<T100Market> collector) throws Exception {

        }
    }

    public static class SegmentParser implements FlatMapFunction<String, T100Segment> {

        @Override
        public void flatMap(String s, Collector<T100Segment> collector) throws Exception {

        }
    }
}

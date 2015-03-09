package de.tuberlin.dima.ti.parser;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class DB1BParser {

    public static class TicketParser implements FlatMapFunction<String, Ticket> {

        private static final String HEADER = "ItinID";
        private static final String DELIM = ",";

        private String[] tmp;

        @Override
        public void flatMap(String s, Collector<Ticket> collector) throws Exception {
            if(s.startsWith(HEADER)) {
                return;
            }
            tmp = s.replaceAll("\"", "").split(DELIM);
        }
    }

    public static class CouponParser implements FlatMapFunction<String, Coupon> {

        @Override
        public void flatMap(String s, Collector<Coupon> collector) throws Exception {

        }
    }

    public static class MarketParser implements FlatMapFunction<String, Market> {

        @Override
        public void flatMap(String s, Collector<Market> collector) throws Exception {

        }
    }


}

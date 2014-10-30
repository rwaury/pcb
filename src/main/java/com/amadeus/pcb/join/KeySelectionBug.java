package com.amadeus.pcb.join;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class KeySelectionBug {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<T3, T3>> ds = env.fromElements(new Tuple2<T3, T3>(new T3(0,1,0), new T3(1,2,0)),
                new Tuple2<T3, T3>(new T3(1,1,0), new T3(2,2,0)),
                new Tuple2<T3, T3>(new T3(2,1,0), new T3(3,2,0)),
                new Tuple2<T3, T3>(new T3(2,1,0), new T3(4,2,0)),
                new Tuple2<T3, T3>(new T3(4,1,0), new T3(1,2,0)),
                new Tuple2<T3, T3>(new T3(3,1,0), new T3(2,2,0)),
                new Tuple2<T3, T3>(new T3(5,1,0), new T3(4,2,0)));

        /* BUGGY CODE START */
        DataSet<Tuple2<Tuple2<T3, T3>,Tuple2<T3, T3>>> result = ds.join(ds).where("f1.f0").equalTo("f0.f0");
        result.print(); // erroneous result
        /* BUGGY CODE END */

        KeySelector<Tuple2<T3, T3>, Integer> jk1 = new KeySelector<Tuple2<T3, T3>, Integer>() {
            public Integer getKey(Tuple2<T3, T3> tuple) {
                return tuple.f1.f0;
            }
        };
        KeySelector<Tuple2<T3, T3>, Integer> jk2 = new KeySelector<Tuple2<T3, T3>, Integer>() {
            public Integer getKey(Tuple2<T3, T3> tuple) {
                return tuple.f0.f0;
            }
        };
        DataSet<Tuple2<Tuple2<T3, T3>,Tuple2<T3, T3>>> result2 = ds.join(ds).where(jk1).equalTo(jk2);
        result2.print(); // expected result

        env.execute();
    }

    public static class T3 extends Tuple3<Integer, Integer, Integer> {
        public T3() {}

        public T3(int i0, int i1, int i2) {
            this.f0 = i0;
            this.f1 = i1;
            this.f2 = i2;
        }
    }
}

package com.amadeus.pcb.join;

import java.nio.charset.Charset;

import com.amadeus.pcb.spargel.TupleInputFormat;
import com.amadeus.pcb.spargel.TupleOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Date;

public class FlightOutput {

    private static final char DELIM = ',';
    private static final int NEWLINE = '\n';
    private static final Charset charset = Charset.forName("UTF-8");

    public static class NonStopFlightOutputFormat extends TextOutputFormat<Flight> {

        public NonStopFlightOutputFormat(Path outputPath) {
            super(outputPath);
        }

        @Override
        public void writeRecord(Flight record) throws IOException {
            byte[] bytes = writeFlightToString(record).getBytes(charset);
            this.stream.write(bytes);
            this.stream.write(NEWLINE);
        }
    }

    public static class TwoLegFlightOutputFormat extends TextOutputFormat<Tuple2<Flight, Flight>> {

        public TwoLegFlightOutputFormat(Path outputPath) {
            super(outputPath);
        }

        @Override
        public void writeRecord(Tuple2<Flight, Flight> record) throws IOException {
            byte[] bytes0 = writeFlightToString(record.f0).getBytes(charset);
            byte[] bytes1 = writeFlightToString(record.f1).getBytes(charset);
            this.stream.write(bytes0);
            this.stream.write(DELIM);
            this.stream.write(bytes1);
            this.stream.write(NEWLINE);
        }
    }

    public static class ThreeLegFlightOutputFormat extends TextOutputFormat<Tuple3<Flight, Flight, Flight>> {

        public ThreeLegFlightOutputFormat(Path outputPath) {
            super(outputPath);
        }

        @Override
        public void writeRecord(Tuple3<Flight, Flight, Flight> record) throws IOException {
            byte[] bytes0 = writeFlightToString(record.f0).getBytes(charset);
            byte[] bytes1 = writeFlightToString(record.f1).getBytes(charset);
            byte[] bytes2 = writeFlightToString(record.f2).getBytes(charset);
            this.stream.write(bytes0);
            this.stream.write(DELIM);
            this.stream.write(bytes1);
            this.stream.write(DELIM);
            this.stream.write(bytes2);
            this.stream.write(NEWLINE);
        }
    }

    public static String writeFlightToString(Flight flight) {
        Date departure = new Date(flight.getDepartureTimestamp());
        Date arrival = new Date(flight.getArrivalTimestamp());
        return flight.getOriginAirport() + DELIM + flight.getDestinationAirport() + DELIM +
                departure.toString() + DELIM + arrival.toString() + DELIM +
                flight.getAirline() + flight.getFlightNumber() + DELIM +
                flight.getLegCount() + DELIM + flight.getCodeShareInfo();
    }

    public static class NonStopFullOutputFormat extends TupleOutputFormat<Flight> {

        @Override
        protected void serialize(Flight record, DataOutputView out) throws IOException {
            record.f0.write(out);
            out.writeLong(record.f1);
            record.f2.write(out);
            out.writeLong(record.f3);
            out.writeUTF(record.f4);
            out.writeInt(record.f5);
            out.writeUTF(record.f6);
            out.writeInt(record.f7);
            out.writeUTF(record.f8);
            out.writeChar(record.f9);
            record.f10.write(out);
            out.writeInt(record.f11);
            out.writeInt(record.f12);
            out.writeInt(record.f13);
            out.writeInt(record.f14);
        }
    }

    public static class NonStopFullInputFormat extends TupleInputFormat<Flight> {

        @Override
        protected Flight deserialize(Flight reuse, DataInputView in) throws IOException {
            reuse.f0.read(in);
            reuse.f1 = in.readLong();
            reuse.f2.read(in);
            reuse.f3 = in.readLong();
            reuse.f4 = in.readUTF();
            reuse.f5 = in.readInt();
            reuse.f6 = in.readUTF();
            reuse.f7 = in.readInt();
            reuse.f8 = in.readUTF();
            reuse.f9 = in.readChar();
            reuse.f10.read(in);
            reuse.f11 = in.readInt();
            reuse.f12 = in.readInt();
            reuse.f13 = in.readInt();
            reuse.f14 = in.readInt();
            return reuse;
        }
    }
}

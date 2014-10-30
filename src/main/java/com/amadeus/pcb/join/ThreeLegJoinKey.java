package com.amadeus.pcb.join;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Key;

import java.io.IOException;

/**
 * Created by robert on 13/10/14.
 */
public abstract class ThreeLegJoinKey implements Key<ThreeLegJoinKey> {

    protected String origin;
    protected String destination;
    protected String airline;
    protected int flightNumber;
    protected long departureTimestamp;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ThreeLegJoinKey)) return false;

        ThreeLegJoinKey that = (ThreeLegJoinKey) o;

        if (departureTimestamp != that.departureTimestamp) return false;
        if (flightNumber != that.flightNumber) return false;
        if (!airline.equals(that.airline)) return false;
        if (!destination.equals(that.destination)) return false;
        if (!origin.equals(that.origin)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = origin.hashCode();
        result = 31 * result + destination.hashCode();
        result = 31 * result + airline.hashCode();
        result = 31 * result + flightNumber;
        result = 31 * result + (int) (departureTimestamp ^ (departureTimestamp >>> 32));
        return result;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeUTF(this.origin);
        out.writeUTF(this.destination);
        out.writeUTF(this.airline);
        out.writeInt(this.flightNumber);
        out.writeLong(this.departureTimestamp);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.origin = in.readUTF();
        this.destination = in.readUTF();
        this.airline = in.readUTF();
        this.flightNumber = in.readInt();
        this.departureTimestamp = in.readLong();
    }
}

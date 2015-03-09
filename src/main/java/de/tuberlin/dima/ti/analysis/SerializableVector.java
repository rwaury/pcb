package de.tuberlin.dima.ti.analysis;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.lang.reflect.Array;

public class SerializableVector implements Value {

    private ArrayRealVector vector = null;

    public SerializableVector() {}

    public SerializableVector(int dimension) {
        this.vector = new ArrayRealVector(dimension);
    }

    public SerializableVector(ArrayRealVector vector) {
        this.vector = vector;
    }

    public ArrayRealVector getVector() {
        return this.vector;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeInt(this.vector.getDimension());
        for(int i = 0; i < this.vector.getDimension(); i++) {
            out.writeDouble(this.vector.getEntry(i));
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        int dimension = in.readInt();
        this.vector = new ArrayRealVector(dimension);
        for(int i = 0; i < dimension; i++) {
            this.vector.setEntry(i, in.readDouble());
        }
    }
}

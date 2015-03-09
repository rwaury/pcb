package de.tuberlin.dima.ti.analysis;


import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;

/**
 * Created by robert on 09/02/15.
 */
public class SerializableMatrix implements Value{

    private Array2DRowRealMatrix matrix = null;

    public SerializableMatrix() {}

    public SerializableMatrix(int dimension) {
        this.matrix = new Array2DRowRealMatrix(dimension, dimension);
    }

    public SerializableMatrix(Array2DRowRealMatrix matrix) {
        this.matrix = matrix;
    }

    public Array2DRowRealMatrix getMatrix() {
        return this.matrix;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        if(this.matrix.getColumnDimension() != this.matrix.getRowDimension()) {
            throw new IOException("Mismatched dimensions! Expected square matrix.");
        }
        out.writeInt(this.matrix.getColumnDimension());
        for(int i = 0; i < this.matrix.getRowDimension(); i++) {
            for(int j = 0; j < this.matrix.getColumnDimension(); j++) {
                out.writeDouble(this.matrix.getEntry(i,j));
            }
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        int dimension = in.readInt();
        this.matrix = new Array2DRowRealMatrix(dimension, dimension);
        for(int i = 0; i < dimension; i++) {
            for(int j = 0; j < dimension; j++) {
                this.matrix.setEntry(i,j, in.readDouble());
            }
        }
    }
}

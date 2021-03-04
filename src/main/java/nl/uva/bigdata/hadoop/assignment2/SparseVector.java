package nl.uva.bigdata.hadoop.assignment2;

import nl.uva.bigdata.hadoop.exercise2.Vector;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SparseVector implements Vector, Writable {
    public int dimension;
    public double[] values;

    @Override
    public int dimension() {
        return this.dimension;
    }

    @Override
    public double get(int i){
        return this.values[i];
    }

    @Override
    public double dot(Vector other) {
        if(other.dimension() != this.dimension()){
            return 0;
        }

        double product = 0;
        for(int i=0; i<this.dimension; i++){
            product += this.get(i) * other.get(i);
        }

        return product;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(dimension);
        out.writeInt(values.length);
        for (double value : values) {
            out.writeDouble(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dimension = in.readInt();
        int size = in.readInt();
        this.values = new double[size];
        for (int n = 0; n < size; n++) {
            this.values[n] = in.readDouble();
        }
    }
}

package nl.uva.bigdata.hadoop.exercise2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class DenseVector implements Vector, Writable {

    public double[] values = new double[0];

    @Override
    public int dimension() {
        return values.length;
    }

    @Override
    public double get(int i){
        return this.values[i];
    }

    @Override
    public double dot(Vector other) {
        if(this.dimension() == other.dimension()){
            double sum = 0;

            for(int i=0; i<this.dimension(); i++){
                sum += this.get(i) * other.get(i);
            }
            return sum;
        }
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for(Double val : values){
            out.writeDouble(val);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int n = in.readInt();
        for(int i=0; i<n; i++){
            this.values[i] = in.readDouble();
        }
    }
}

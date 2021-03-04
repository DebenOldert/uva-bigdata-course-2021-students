package nl.uva.bigdata.hadoop.assignment2;

import com.sun.rowset.internal.Row;
import nl.uva.bigdata.hadoop.HadoopJob;
import nl.uva.bigdata.hadoop.exercise2.DenseVector;
import nl.uva.bigdata.hadoop.exercise2.Vector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

public class SparseMatrixVectorMultiplication extends HadoopJob {

    @Override
    public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

        Map<String,String> parsedArgs = parseArgs(args);

        Path matrix = new Path(parsedArgs.get("--matrix"));
        Path vector = new Path(parsedArgs.get("--vector"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        Job multiplication = prepareJob(onCluster, jobConf,
                matrix, outputPath, TextInputFormat.class, RowDotProductMapper.class,
                Text.class, NullWritable.class, TextOutputFormat.class);

        multiplication.addCacheFile(vector.toUri());
        multiplication.addCacheFile(matrix.toUri());
        multiplication.waitForCompletion(true);

        return 0;
    }

    static class RowDotProductMapper extends Mapper<Object, Text, Text, NullWritable> {
        private static final Pattern SEPARATOR = Pattern.compile("\t");
        private static final Pattern VALUE_SEPARATOR = Pattern.compile(";");
        private static final DenseVector VECTOR = new DenseVector();
        private static final Text OUTPUT = new Text();
        private int num_rows = 0;

        private double[] fromString(String encoded) {
            String[] tokens = VALUE_SEPARATOR.split(encoded);
            int dimension = tokens.length;
            double[] values = new double[dimension];
            for (int n = 0; n < dimension; n++) {
                values[n] = Double.parseDouble(tokens[n]);
            }
            return values;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI vectorsFile = context.getCacheFiles()[0];
            FileSystem fs = FileSystem.get(vectorsFile, context.getConfiguration());

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(vectorsFile))))) {
                String line = reader.readLine();
                // System.out.println(line);
                // if(VECTOR.dimension() / VECTOR.values.length > 0.5){
                VECTOR.values = fromString(line);
            }
            URI matrixFile = context.getCacheFiles()[1];
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(matrixFile))))) {
                reader.readLine();
                num_rows++;
            }

        }

        public void map(Object key, Text value, Mapper.Context ctx) throws IOException, InterruptedException {
            String[] tokens = SEPARATOR.split(value.toString());
            int rowIndex = Integer.parseInt(tokens[0]);
            System.out.println(value.toString());

            double[] values = fromString(tokens[1]);

//            int size = num_rows * values.length;
//            int zeros = 0;
//
//            for(int i=0; i<num_rows; i++){
//                for(int j=0; j<values.length; j++){
//                    if()
//                }
//            }


            DenseVector row = new DenseVector();
            row.values = values;

            double dotProduct = row.dot(VECTOR);

            OUTPUT.set(rowIndex + "\t" + dotProduct);
            ctx.write(OUTPUT, NullWritable.get());

        }
    }
}

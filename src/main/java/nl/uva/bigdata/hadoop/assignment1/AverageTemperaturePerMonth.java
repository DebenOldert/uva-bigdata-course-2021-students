package nl.uva.bigdata.hadoop.assignment1;


import com.google.common.collect.Iterators;
import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class AverageTemperaturePerMonth extends HadoopJob {

  @Override
  public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

    Job temperatures = prepareJob(onCluster, jobConf,
        inputPath, outputPath, TextInputFormat.class, MeasurementsMapper.class,
        YearMonthWritable.class, IntWritable.class, AveragingReducer.class, Text.class,
        NullWritable.class, TextOutputFormat.class);

    temperatures.getConfiguration().set("__UVA_minimumQuality", Double.toString(minimumQuality));

    temperatures.waitForCompletion(true);

    return 0;
  }

  static class YearMonthWritable implements WritableComparable {

    private int year;
    private int month;

    public YearMonthWritable() {}

    public int getYear() {
      return year;
    }

    public void setYear(int year) {
      this.year = year;
    }

    public int getMonth() {
      return month;
    }

    public void setMonth(int month) {
      this.month = month;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(getYear());
      out.writeInt(getMonth());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      setYear(in.readInt());
      setMonth(in.readInt());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      YearMonthWritable that = (YearMonthWritable) o;
      return year == that.year && month == that.month;
    }

    @Override
    public int hashCode() {
      return Objects.hash(year, month);
    }

    @Override
    public int compareTo(Object o) {
      YearMonthWritable other = (YearMonthWritable) o;
      int byYear = Integer.compare(this.year, other.year);

      if (byYear == 0) {
        return Integer.compare(this.month, other.month);
      } else {
        return byYear;
      }
    }
  }

  public static class MeasurementsMapper extends Mapper<Object, Text, YearMonthWritable, IntWritable> {
    private final static IntWritable TEMP = new IntWritable(0);
    private final YearMonthWritable YM = new YearMonthWritable();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

      String[] parts = value.toString().split("\t");

      YM.setYear(Integer.parseInt(parts[0]));
      YM.setMonth(Integer.parseInt(parts[1]));

      Integer temp = Integer.parseInt(parts[2]);
      Double quality = Double.parseDouble(parts[3]);

      Double minQ = context.getConfiguration().getDouble("__UVA_minimumQuality", 0);

      if(quality >= minQ){
        TEMP.set(temp);
        context.write(YM, TEMP);
      }

    }
  }

  public static class AveragingReducer extends Reducer<YearMonthWritable,IntWritable,Text,NullWritable> {

    private final Text word = new Text();

    public void reduce(YearMonthWritable yearMonth, Iterable<IntWritable> temperatures, Context context)
            throws IOException, InterruptedException {
      int n = 0;

      int s = 0;
      for (IntWritable temp: temperatures){
        s += temp.get();
        n++;
      }

      double avg = ((double)s / (double)n);

      word.set(yearMonth.year + "\t"+ yearMonth.month + "\t" + avg);

      context.write(word, NullWritable.get());

    }
  }
}
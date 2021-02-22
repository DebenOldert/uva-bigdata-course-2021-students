package nl.uva.bigdata.hadoop.assignment1;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;


public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

    Map<String, String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job job;

    if (onCluster) {
      job = new Job(jobConf);
    } else {
      job = new Job();
    }

    Configuration conf = job.getConfiguration();

    job.setReducerClass(AuthorBookJoinReducer.class);
    job.setJarByClass(BookAndAuthorReduceSideJoin.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    MultipleInputs.addInputPath(job, authors, TextInputFormat.class, AuthorsMapper.class);
    MultipleInputs.addInputPath(job, books, TextInputFormat.class, BooksMapper.class);

    FileOutputFormat.setOutputPath(job, outputPath);
    //outputPath.getFileSystem(conf).delete(outputPath);
    job.waitForCompletion(true);

    return 0;
  }

  static class BookWritable implements WritableComparable {

    private int year;
    private int author_id;
    private String title;

    public BookWritable(){}

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(year);
      out.writeInt(author_id);
      out.writeUTF(title);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      year = in.readInt();
      author_id = in.readInt();
      title = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BookWritable that = (BookWritable) o;
      return year == that.year && author_id == that.author_id && title.equals(that.title);
    }

    @Override
    public int hashCode() { return Objects.hash(year, author_id, title);}

    @Override
    public int compareTo(Object o) {
      BookWritable other = (BookWritable) o;

      int byAuthor = Integer.compare(author_id, other.author_id);

      if(byAuthor == 0){
        int byYear = Integer.compare(year, other.year);
//        if(byYear == 0){
//          return title.compareTo(other.title);
//        }
        return byYear;
      }
      return byAuthor;
    }
  }

  static class AuthorWritable implements WritableComparable {

    private int id;
    private String name;

    public AuthorWritable() {};

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(id);
      out.writeUTF(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      id = in.readInt();
      name = in.readUTF();
    }

    @Override
    public boolean equals(Object o){
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AuthorWritable that = (AuthorWritable) o;
      return id == that.id;
    }

    @Override
    public int hashCode() { return Objects.hash(id, name);}

    @Override
    public int compareTo(Object o) {
      AuthorWritable other = (AuthorWritable) o;

      int byID = Integer.compare(id, other.id);
//      if(byID == 0){
//        return name.compareTo(other.name);
//      }
      return byID;
    }
  }

  public static class AuthorsMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Mapper.Context ctx) throws IOException, InterruptedException {
      String[] parts = value.toString().split("\t");

      AuthorWritable AUTHOR = new AuthorWritable();
      Text ID = new Text();
      Text word = new Text();

      AUTHOR.id = Integer.parseInt(parts[0]);
      AUTHOR.name = parts[1];

      ID.set("A-" + AUTHOR.id);
      word.set(AUTHOR.name);
      //ctx.write(ID, AUTHOR);
      ctx.write(ID, word);
    }
  }

  public static class BooksMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Mapper.Context ctx) throws IOException, InterruptedException {
      String[] parts = value.toString().split("\t");

      BookWritable BOOK = new BookWritable();
      Text ID = new Text();
      Text word = new Text();

      BOOK.author_id = Integer.parseInt(parts[0]);
      BOOK.year = Integer.parseInt(parts[1]);
      BOOK.title = parts[2];

      ID.set("B-" + BOOK.author_id);
      word.set(BOOK.title + "\t" + BOOK.year);

      //ctx.write(ID, BOOK);
      ctx.write(ID, word);
    }
  }

  public static class AuthorBookJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
    private final Text word = new Text();
    //private final ArrayList<Text> books = new ArrayList<Text>();
    private final int size = 10;
    String[] authors = new String[size];
    ArrayList<String>[] books = new ArrayList[size];


    public void reduce(Text author_id, Iterable<Text> texts, Context ctx) throws IOException, InterruptedException {

      for(Text text: texts){
        String[] split = author_id.toString().split("-");

        int id = Integer.parseInt(split[1]);

        if(split[0].equals("A")){
          authors[id] = text.toString();
        }
        if(split[0].equals("B")){
          if(books[id] == null){
            books[id] = new ArrayList<>();
          }
          books[id].add(text.toString());
        }
      }

      for(int i = 0; i<size; i++){
        if(books[i] != null && authors[i] != null){
          for(String b: books[i]){
            word.set(authors[i] + "\t" + b);
            ctx.write(word, NullWritable.get());
          }
        }
      }
    }
  }
}
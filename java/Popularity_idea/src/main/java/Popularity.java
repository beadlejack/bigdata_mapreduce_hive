// Immanuel Amirtharaj, Jackson Beadle
// COEN 242 -- Project 1
// Part 1
// MapReduce jobs to sort movies by popularity (# of reviews)
// Two MR jobs: one to count the reviews, one to sort by popularity

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;



public class Popularity {

    // Mapper to count reviews, adapts WordCount problem (Job1)
    // <key, value> = <IntWritable movieId, Text pop\t1>
    public static class MapForPopularity extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");
            if (!words[0].equals("userId")) {   // skip header row
                IntWritable outputKey = new IntWritable(Integer.parseInt(words[1]));
                con.write(outputKey, new Text("pop\t1"));
            }
        }
    }

    // Mapper to get movie title (Job1)
    // <key, value> = <IntWritable movieId, Text title\tTitle>
    public static class MapForTitle extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            // regex to only split on comma if not preceded w/ odd # of double quotes src:
            // https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
            if (!words[0].equals("movieId")) {  // skip header row
                IntWritable outputKey = new IntWritable(Integer.parseInt(words[0]));
                String out = words[1];

                // remove "" quotes from beginning/end of title field
                // to match how field is read in Hive
                if (out.charAt(0) == '\"') {
                    out = out.substring(1, out.length());
                }
                if (out.charAt(out.length()-1) == '\"') {
                    out = out.substring(0, out.length() - 1);
                }
                con.write(outputKey, new Text("title\t" + out));
            }
        }
    }

    // Reducer for Job1, gives unsorted output
    // <key, value> = <IntWritable reviewCount, Text title>
    // This is the input for Job2
    public static class ReduceForPopularity extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context con) throws IOException,
                InterruptedException {
            int sum = 0;
            String title = "";
            for (Text value : values) {
                String[] vals = value.toString().split("\t", 2);

                // review value
                if (vals[0].equals("pop"))
                    sum++;
                // title value
                else {
                    title = vals[1];
                }
            }
            if (sum > 0 ) {
                // only write output if there is a review since Hive outputs count > 0
                // This will be our input to the next job
                con.write(new IntWritable(sum), new Text(title));
            }
        }
    }

    // Mapper for Job2 to sort output by reviewCount
    // <key, value> = <Text reviewCount\tTitle, Text title>
    public static class DumbMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {


            String line = value.toString();
            String[] words = line.split("\t", 2);

            // combinator key to sort by reviewCount AND title
            // don't want movies with same reviewCount grouped in intermediate
            Text outputKey = new Text(line);
            Text outputValue = new Text(words[1]);
            con.write(outputKey, outputValue);

        }
    }

    // Reducer for Job2 to output sorted results
    // <key, value> = <IntWritable reviewCount, Text title>
    public static class DumbReducer extends Reducer<Text, Text, IntWritable, Text> {
        public void reduce(Text key, Iterable<Text> values, Context con) throws IOException,
                InterruptedException {

            // extract reviewCount from combinator key
            String[] words = key.toString().split("\t", 2);
            IntWritable outputKey = new IntWritable(Integer.parseInt(words[0]));
            con.write(outputKey, values.iterator().next());
        }
    }

    // MapReduce driver
    public static void main(String[] args) throws Exception {
        // Get input argument and setup configuration
        Configuration config = new Configuration();
        config.set("mapred.textoutputformat.separatorText", ",");
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

        // setup mapreduce job
        Job job = new Job(config,"popularity");
        job.setJarByClass(Popularity.class);
        job.setReducerClass(ReduceForPopularity.class);

        // set input/output paths
        Path input = new Path(files[0]);
        Path input2 = new Path(files[1]);
        Path output = new Path(files[2]);
        Path outputSorted = new Path(files[3]);

        // set output types
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // set up mappers, output path
        MultipleInputs.addInputPath(job, input, TextInputFormat.class, MapForTitle.class);
        MultipleInputs.addInputPath(job, input2, TextInputFormat.class, MapForPopularity.class);
        FileOutputFormat.setOutputPath(job, output);

        // wait for first job to complete before sort job starts
        job.waitForCompletion(true);

        // The second hadoop job to sort the output by reviewCount instead of movieId
        Configuration sorterConfig = new Configuration();
        Job sorterJob = new Job(sorterConfig, "popularitySorter");

        // set output types
        sorterJob.setMapOutputKeyClass(Text.class);
        sorterJob.setOutputKeyClass(IntWritable.class);
        sorterJob.setOutputValueClass(Text.class);

        // set mapper/reducer
        sorterJob.setMapperClass(DumbMapper.class);
        sorterJob.setReducerClass(DumbReducer.class);

        // set comparator to sort keys in second job by reviewCount AND title
        sorterJob.setSortComparatorClass(PopularityComparator.class);

        sorterJob.setJarByClass(Popularity.class);

        // set input/output
        FileInputFormat.addInputPath(sorterJob, output);
        FileOutputFormat.setOutputPath(sorterJob, outputSorted);

        // explicitly set reduce tasks to 1 so output is sorted
        sorterJob.setNumReduceTasks(1);

        // return when done
        System.exit(sorterJob.waitForCompletion(true) ? 0 : 1);

    }
}
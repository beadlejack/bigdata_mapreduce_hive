// Immanuel Amirtharaj, Jackson Beadle
// COEN 242 -- Project 1
// Part 2
// Query movies with average rating > 4 and reviewCount > 10
// Sort by rating, ascending order
// Two MapReduce jobs, one to compute average, reviewCount, one to sort

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

public class Reviews {

    // Mapper to read rating (Job1)
    // <key, value> = <IntWritable movieId, Text pop\trating>
    public static class MapForReview extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String line = value.toString();

            String[] words = line.split(",");
            if (!words[0].equals("userId")) {   // skip header line
                IntWritable outputKey = new IntWritable(Integer.parseInt(words[1]));
                con.write(outputKey, new Text("pop\t" + words[2]));
            }
        }
    }

    // Mapper to read movie title (Job1)
    // <key, value> = <IntWritable movieId, Text title\tTitle>
    public static class MapForTitle extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",", 2);

            if (!words[0].equals("movieId")) {  // skip header line
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

    // Reducer for Job1 to compute averageRating and reviewCount
    // <key, value> = <Text title, Text averageRating\treviewCount>
    // Unsorted output -> input for Job2
    public static class ReduceForReview extends Reducer<IntWritable, Text, Text, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context con) throws IOException,
                InterruptedException {

            int numReviews = 0;
            double totalScore = 0;
            String title = "";

            for (Text value : values) {
                String[] vals = value.toString().split("\t", 2);

                // case: rating value
                if (vals[0].equals("pop")) {
                    // increment count, add rating to sum
                    numReviews++;
                    totalScore += Double.parseDouble(vals[1]);

                }
                // case: title value
                else {
                    title = vals[1];
                }
            }

            // filter out movies with <=10 reviews and averageRating <4.0
            if (numReviews > 10) {
                double movieAverage = totalScore / (double) numReviews;

                if (movieAverage > 4.0) {
                    String payload = movieAverage + "\t" + numReviews;
                    con.write(new Text(title), new Text(payload));

                }
            }
        }
    }

    // Mapper for Job2 to sort output from Job1 by averageRating
    // <key, value> = <Text title\taverageRating, Text averageRating\treviewCount>
    public static class DumbMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            // split line into fields
            String line = value.toString();
            String[] words = line.split("\t", 3);

            // set output key and value
            Text outputKey = new Text(words[0] + "\t" + words[1]);
            Text outputValue = new Text(words[1] + "\t" + words[2]);
            con.write(outputKey, outputValue);

        }
    }

    // Reducer for Job2 to output sorted results
    // <key, value> = <Text title, Text averageRating\treviewCount>
    public static class DumbReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context con) throws IOException,
                InterruptedException {

            // extract title from key, output results
            String[] words = key.toString().split("\t", 2);
            con.write(new Text(words[0]), values.iterator().next());
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        // Get input argument and setup configuration
        Configuration config = new Configuration();
       // config.set("mapred.textoutputformat.separatorText", ",");
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();
        // setup mapreduce job
        Job job = new Job(config,"popularity");
        job.setJarByClass(Reviews.class);

        // set input/output path
        Path input = new Path(files[0]);
        Path input2 = new Path(files[1]);
        Path output = new Path(files[2]);
        Path outputSorted = new Path(files[3]);

        // set output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // set up mappers, reducer output
        MultipleInputs.addInputPath(job, input, TextInputFormat.class, MapForTitle.class);
        MultipleInputs.addInputPath(job, input2, TextInputFormat.class, MapForReview.class);
        FileOutputFormat.setOutputPath(job, output);
        job.setReducerClass(ReduceForReview.class);

        // wait for Job1 completes before starting Job2
        job.waitForCompletion(true);

        // The second hadoop job to sort output by averageRating
        Configuration sorterConfig = new Configuration();
        Job sorterJob = new Job(sorterConfig, "popularitySorter");

        // set output types
        sorterJob.setMapOutputKeyClass(Text.class);
        sorterJob.setMapOutputKeyClass(Text.class);
        sorterJob.setOutputKeyClass(Text.class);
        sorterJob.setOutputValueClass(Text.class);

        // set mapper/reducer
        sorterJob.setMapperClass(DumbMapper.class);
        sorterJob.setReducerClass(DumbReducer.class);
        // set comparator class to sort keys in second job by averageRating THEN title
        sorterJob.setSortComparatorClass(ReviewComparator.class);

        sorterJob.setJarByClass(Reviews.class);

        // explicitly set reduce tasks to 1 so output is sorted
        sorterJob.setNumReduceTasks(1);

        // set input/output paths
        FileInputFormat.addInputPath(sorterJob, output);
        FileOutputFormat.setOutputPath(sorterJob, outputSorted);

        // return when done
        System.exit(sorterJob.waitForCompletion(true) ? 0 : 1);
    }
}
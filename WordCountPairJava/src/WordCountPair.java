

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountPair {

    public static void main(String[] args) throws Exception {

        System.out.print("Word Count Pair to print Counts of pairs of words");

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        /** Entry-point for our program. Constructs a Job object representing a single
         * Map-Reduce job and asks Hadoop to run it.
         *
         */

        Job job = new Job(conf, "word count");

        /*Initialize the Hadoop job and set the jar as well as the name of the Job
        * Tell Hadoop where to locate the code that must be shipped if this
        * job is to be run across a cluster.
        */
        job.setJarByClass(WordCountPair.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

         /* Set the datatypes of the keys and values outputted by the maps and reduces.
         * These must agree with the types used by the Mapper and Reducer. Mismatches
         * will not be caught until runtime.
         *
         * Read from StackOverflow:
         * job.setOutputKeyClass( Text.class ); will set the types expected as output from both the map and reduce phases.
         *If your Mapper emits different types than the Reducer,
         * you can set the types emitted by the mapper with the JobConf's setMapOutputKeyClass() and setMapOutputValueClass() methods.
         * These implicitly set the input types expected by the Reducer.
         */

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    //Mapper class to convert input key Value pairs to intermediate key value pairs.
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        /** Regex pattern to find pairs of words (alphanumeric + _). */
        final static Pattern WORD_PATTERN = Pattern.compile("(\\w+)(?=(\\s\\w+))");

        private final static IntWritable one = new IntWritable(1);
        /** Text object to store a word to write to output. */
        private Text word = new Text();
        // context is a reference to work with
        // link between the datanodes
        /** Actual map function. Takes one document's text and emits key-value
         * pairs for each word found in the document.
         *
         * @param key Document identifier (ignored).
         * @param value Text of the current document.
         * @param context MapperContext object for accessing output,
         *                configuration information, etc.
         */

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Take the intermediate string
            String intermediate=value.toString();
            //Converting to lower case.
            String lower=intermediate.toLowerCase();

            //Replace all special characters with spaces in regex.
            String modified=lower.replaceAll("[^a-zA-Z0-9]", " ");

            //To replaced multiple spaces with single space in the input
            String INPUT= modified.trim().replaceAll(" +", " ");

            //If the line I read is not just an empty line or many new line characters

            if(!(INPUT=="\n+" && INPUT=="")){
                //all words in the line
                String[] words=INPUT.split("\\s");
                for(int i=0;i<words.length;i++){

                    //Last word just emit as (WORD,one)
                    if(i==words.length-1){
                        word.set(words[i]);
                        context.write(word, one);
                    }
                    //just make consecutive pairs and write to output
                    else{
                        String temp=words[i]+" "+words[i+1];
                        word.set(temp);
                        context.write(word, one);

                    }
                }

            }

            
        }
    }

    /** Reducer for word count pair.
     *
     * Like the Mapper base class, the base class Reducer is parameterized by
     * <in key type, in value type, out key type, out value type>.
     *
     * For each Text key, which represents a pair of word, this reducer gets a list of
     * IntWritable values, computes the sum of those values, and the key-value
     * pair (word, sum).
     */

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}

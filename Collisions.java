import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class Collisions extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Collisions.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Collisions(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());

        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {


        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                if (key.get() == 0)
                    return;
                else {
                    String[] line = value.toString().split(",");
                    Integer currentYear = Integer.parseInt(line[0].substring(line[0].lastIndexOf('/') + 1,
                            line[0].lastIndexOf('/') + 5));
                    String keyOut = line[2] + "\t";
                    if (currentYear > 2012 && !line[2].isEmpty()) {
                        if (!line[11].isEmpty() && !line[11].equals("0")) {
                            if (!line[6].isEmpty()) {
                                context.write(new Text(keyOut  + line[6] + "\t" + "PEDESTRIAN\tINJURED"), new IntWritable(Integer.parseInt(line[11])));
                            }
                            if (!line[7].isEmpty()) {
                                context.write(new Text(keyOut + line[7] + "\t" + "PEDESTRIAN\tINJURED"), new IntWritable(Integer.parseInt(line[11])));
                            }
                            if (!line[8].isEmpty()) {
                                context.write(new Text(keyOut + line[8] + "\t" + "PEDESTRIAN\tINJURED"), new IntWritable(Integer.parseInt(line[11])));
                            }
                        }
                        if (!line[12].isEmpty() && !line[12].equals("0")) {
                            if (!line[6].isEmpty()) {
                                context.write(new Text(keyOut  + line[6] + "\t" + "PEDESTRIAN\tKILLED"), new IntWritable(Integer.parseInt(line[12])));
                            }
                            if (!line[7].isEmpty()) {
                                context.write(new Text(keyOut + line[7] + "\t" + "PEDESTRIAN\tKILLED"), new IntWritable(Integer.parseInt(line[12])));
                            }
                            if (!line[8].isEmpty()) {
                                context.write(new Text(keyOut + line[8] + "\t" + "PEDESTRIAN\tKILLED"), new IntWritable(Integer.parseInt(line[12])));
                            }
                        }
                        if (!line[13].isEmpty() && !line[13].equals("0")) {
                            if (!line[6].isEmpty()) {
                                context.write(new Text(keyOut  + line[6] + "\t" + "CYCLIST\tINJURED"), new IntWritable(Integer.parseInt(line[13])));
                            }
                            if (!line[7].isEmpty()) {
                                context.write(new Text(keyOut + line[7] + "\t" + "CYCLIST\tINJURED"), new IntWritable(Integer.parseInt(line[13])));
                            }
                            if (!line[8].isEmpty()) {
                                context.write(new Text(keyOut + line[8] + "\t" + "CYCLIST\tINJURED"), new IntWritable(Integer.parseInt(line[13])));
                            }

                        }
                        if (!line[14].isEmpty() && !line[14].equals("0")) {
                            if (!line[6].isEmpty()) {
                                context.write(new Text(keyOut  + line[6] + "\t" + "CYCLIST\tKILLED"), new IntWritable(Integer.parseInt(line[14])));
                            }
                            if (!line[7].isEmpty()) {
                                context.write(new Text(keyOut + line[7] + "\t" + "CYCLIST\tKILLED"), new IntWritable(Integer.parseInt(line[14])));
                            }
                            if (!line[8].isEmpty()) {
                                context.write(new Text(keyOut + line[8] + "\t" + "CYCLIST\tKILLED"), new IntWritable(Integer.parseInt(line[14])));
                            }
                        }
                        if (!line[15].isEmpty() && !line[15].equals("0")) {
                            if (!line[6].isEmpty()) {
                                context.write(new Text(keyOut  + line[6] + "\t" + "MOTORIST\tINJURED"), new IntWritable(Integer.parseInt(line[15])));
                            }
                            if (!line[7].isEmpty()) {
                                context.write(new Text(keyOut + line[7] + "\t" + "MOTORIST\tINJURED"), new IntWritable(Integer.parseInt(line[15])));
                            }
                            if (!line[8].isEmpty()) {
                                context.write(new Text(keyOut + line[8] + "\t" + "MOTORIST\tINJURED"), new IntWritable(Integer.parseInt(line[15])));
                            }
                        }
                        if (!line[16].isEmpty() && !line[16].equals("0")) {
                            if (!line[6].isEmpty()) {
                                context.write(new Text(keyOut  + line[6] + "\t" + "MOTORIST\tKILLED"), new IntWritable(Integer.parseInt(line[16])));
                            }
                            if (!line[7].isEmpty()) {
                                context.write(new Text(keyOut + line[7] + "\t" + "MOTORIST\tKILLED"), new IntWritable(Integer.parseInt(line[16])));
                            }
                            if (!line[8].isEmpty()) {
                                context.write(new Text(keyOut + line[8] + "\t" + "MOTORIST\tKILLED"), new IntWritable(Integer.parseInt(line[16])));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
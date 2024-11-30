package tn.enit.deIlliteracy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class EducationLevelCount {

    private static final Logger logger = LogManager.getLogger(EducationLevelCount.class);
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count People by Education Level");
        job.setJarByClass(EducationLevelCount.class);

        job.setMapperClass(EducationCountMapper.class);
        logger.info("Mapper done");
        job.setReducerClass(EducationCountReducer.class);
        logger.info("reducer done");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Logger log = Logger.getLogger(EducationLevelCount.class.getName());
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
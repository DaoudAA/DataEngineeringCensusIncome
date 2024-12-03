package tn.enit.computerhard;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

public class ReliabilityPerVendor {

    private static final Logger logger = LogManager.getLogger(ReliabilityPerVendor.class);
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average reliablity per vendor");
        job.setJarByClass(ReliabilityPerVendor.class);
        job.setMapperClass(AvgReliabilityMapper.class);
        logger.info("Mapper done");
        job.setCombinerClass(AvgReliabilityCombiner.class);
        logger.info("Combiner done");
        job.setReducerClass(AvgReliabilityReducer.class);
        logger.info("reducer done");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Logger log = Logger.getLogger(ReliabilityPerVendor.class.getName());
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
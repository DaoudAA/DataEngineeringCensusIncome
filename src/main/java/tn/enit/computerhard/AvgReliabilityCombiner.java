package tn.enit.computerhard;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import java.io.IOException;

public class AvgReliabilityCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private static final Logger logger = LogManager.getLogger(AvgReliabilityCombiner.class);

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;

        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
        logger.info("Combiner: Processing vendor: " + key + " with local average: " + (sum / count));

        context.write(key, new DoubleWritable(sum / count));
    }
}

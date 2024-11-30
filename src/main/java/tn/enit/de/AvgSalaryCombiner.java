package tn.enit.de;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import java.io.IOException;

public class AvgSalaryCombiner  extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private static final Logger logger = LogManager.getLogger(AvgSalaryCombiner.class);

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;

        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
        logger.info("Combiner : Processing line: " + key +" has this number of ppl : "+ count);

        context.write(key, new DoubleWritable(sum / count));
    }
}
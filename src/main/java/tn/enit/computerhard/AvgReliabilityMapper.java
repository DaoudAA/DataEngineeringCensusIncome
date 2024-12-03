package tn.enit.computerhard;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import java.io.IOException;


import java.io.IOException;

public class AvgReliabilityMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text vendor = new Text();
    private DoubleWritable reliability = new DoubleWritable();
    private static final Logger logger = LogManager.getLogger(AvgReliabilityMapper.class);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 10) {
            try {
                vendor.set(fields[0].trim());

                double prp = Double.parseDouble(fields[8].trim());
                double erp = Double.parseDouble(fields[9].trim());
                double reliabilityValue = prp - erp;

                reliability.set(reliabilityValue);
                context.write(vendor, reliability);

                logger.info("MAPPER: Vendor: " + vendor + ", Reliability: " + reliabilityValue);

            } catch (NumberFormatException e) {
                logger.warn("MAPPER: Skipping line due to invalid number format: " + value.toString(), e);
            }
        } else {
            logger.warn("MAPPER: Skipping line due to insufficient columns: " + value.toString());
        }
    }
}
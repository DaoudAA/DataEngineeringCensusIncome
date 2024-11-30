package tn.enit.de;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import java.io.IOException;

public class AvgSalaryMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text maritalStatus = new Text();
    private DoubleWritable salary = new DoubleWritable();
    private static final Logger logger = LogManager.getLogger(AvgSalaryMapper.class);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 12) {
            maritalStatus.set(fields[5].trim());
            salary.set(Double.parseDouble(fields[12].trim()));
            context.write(maritalStatus, salary);
            logger.info("MAPPER: Processing line: " + maritalStatus +" has this salary : "+ salary.toString());
        }
    }
}

package tn.enit.deIlliteracy;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class EducationCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text educationLevel = new Text();
    private static final Logger logger = LogManager.getLogger(EducationCountMapper.class);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 5) {
            educationLevel.set(fields[3].trim());

            context.write(new Text(educationLevel), new IntWritable(1));
            //logger.info("MAPPER: Processing line: " + maritalStatus +" has this salary : "+ salary.toString());
        }
    }
}

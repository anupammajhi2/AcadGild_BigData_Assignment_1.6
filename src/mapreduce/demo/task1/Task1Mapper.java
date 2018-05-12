package mapreduce.demo.task1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task1Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		
		String companyname = lineArray[0];
		String productname = lineArray[1];
		
		if(!(companyname.equals("NA") || productname.equals("NA")) ) {
			context.write(key,value);
		}
		
	}
}

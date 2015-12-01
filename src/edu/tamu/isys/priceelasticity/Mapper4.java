package edu.tamu.isys.priceelasticity;

/* Imports have been organized for Mapper Class */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class Mapper4 extends Mapper<LongWritable, Text, Text, Text>
{
	/* For improving efficiency of the program, variables have been declared outside the methods to optimize the processing */
	private String rawData = "";
	private Text newKey;
	private Text newValue;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		rawData = value.toString();
		
		try
		{
			String[] inputText = rawData.split(" ");
			newKey = new Text(inputText[0]);
			newValue = new Text(inputText[1]);
			context.write(newKey, newValue);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}

package edu.tamu.isys.priceelasticity;

/* Imports have been organized for Mapper Class */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>
{
	/* For improving efficiency of the program, variables have been declared outside the methods to optimize the processing */
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
	}
}

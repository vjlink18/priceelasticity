package edu.tamu.isys.priceelasticity;

/* Imports have been organized for RatingsReducer Class */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> 
{
	/* For improving efficiency of the program, variables have been declared outside the methods to optimize the processing */
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		
	}
}
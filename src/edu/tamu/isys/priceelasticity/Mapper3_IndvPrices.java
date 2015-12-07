package edu.tamu.isys.priceelasticity;

/*
 * This is mapper class to convert the input from file into key value pairs 
 * output format for mapper is as follows 
 * key =  Book , value = list of all the corresponding values 
 * 
 */

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class Mapper3_IndvPrices extends Mapper <LongWritable, Text, Text, Text> 
{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String lineNew = value.toString();
		
		//Spilt the input based on tab to extract DataMonth (DM) and append it to key
		String[] lineNewSplitArray = lineNew.split("\\t");
		if (lineNewSplitArray.length > 1) 
		{
			String inputKey = lineNewSplitArray[0];
			String mapperValue = lineNewSplitArray[1];

			String mapperLine = mapperValue.toString();
			String[] parts = mapperLine.split("_");
			String DM = parts[0];

			// Write output as key and values from input
			String mapperKey = inputKey + "_" + DM;
			context.write(new Text(mapperKey), new Text(mapperValue));
		}
	}
}
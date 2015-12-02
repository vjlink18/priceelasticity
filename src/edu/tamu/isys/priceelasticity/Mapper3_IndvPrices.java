package edu.tamu.isys.priceelasticity;

/*
 * This is mapper class to convert the input from file into key value pairs 
 * output format for mapper is as follows 
 * key =  Book , value = list of all the corresponding values inclusing new calculated book prices
 * 
 */

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.Text;


public class Mapper3_IndvPrices extends Mapper < LongWritable, Text, Text, Text > {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		 String lineNew = value.toString();

			 String[] lineNewSplitArray =lineNew.split(" ") ;
			if(lineNewSplitArray.length >1){
			 
			String inputKey=lineNewSplitArray[0];
			String mapperValue=lineNewSplitArray[1];
			
			String mapperLine = mapperValue.toString();
			String[] parts = mapperLine.split("_"); 
			String	DM	=	parts[0];
			
			// Print out put as key and values from input
			String mapperKey = inputKey + "_" + DM ;
			
			
			context.write(new Text(mapperKey), new Text(mapperValue));
		
				
				}

			
		}
}

	


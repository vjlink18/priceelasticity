package edu.tamu.isys.pricing;

/*
 * This is reducer class to convert the input from Mapper into key value pairs 
 * output format for reducer is as follows 
 * key =  movie genre , value = highest rated "movie name" and its "rating" corresponding to the genre
 * 
 */

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer < Text, Text, Text, Text > {


	public void reduce(Text key,  Text  values, Context context) throws IOException, InterruptedException {

		

		// Print out put as genre name , top rated movie and its average rating in that genre
		context.write(key, new Text(values));
	}

}
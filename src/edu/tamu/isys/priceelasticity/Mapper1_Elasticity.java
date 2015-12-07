package edu.tamu.isys.priceelasticity;

/*
 * This is mapper class to convert the input from a text file into key value pairs 
 * output format for mapper is as follows 
 * key =  BookID , value = list of all the corresponding values.
 */

/* Imports have been organized for Mapper Class */
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1_Elasticity extends Mapper <LongWritable, Text, Text, Text> 
{
	String rawData;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		rawData = value.toString();
		BookEntry bookEntry;
		
		try 
		{
			bookEntry = new BookEntry(rawData);
			
			if (!bookEntry.hasError()) 
			{
				String keyValue = "";
				String bookID = "";
				bookID = bookEntry.getBookID();
				keyValue = bookEntry.getDataMonth() + "_" + bookEntry.getBookName() + "_" + bookEntry.getPrice() + "_" + bookEntry.getQuantity() + "_" + bookEntry.getBookType() + "_" + bookEntry.getOrigPubPrice() + "_" + bookEntry.getGrowthRate() + "_" + bookEntry.getPublishYear();
				
				// BookId is the key and a string of Monthweek, price and
				// quantity is the value
				context.write(new Text(bookID), new Text(keyValue));
			}
			
		} 
		catch (ParseException e) 
		{
			e.printStackTrace();
		}

	}
}
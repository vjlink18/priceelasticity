package edu.tamu.isys.priceelasticity;

/* Imports have been organized for Mapper Class */
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1_Elasticity extends Mapper<LongWritable, Text, Text, Text> {
	/*
	 * For improving efficiency of the program, variables have been declared
	 * outside the methods to optimize the processing
	 */

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		System.out.println("Mapper started");
		String rawData = value.toString();
		BookEntry bookEntry;
		try {
			bookEntry = new BookEntry(rawData);
			if (!bookEntry.hasError()) {
				System.out.println("Inside If condition");
				String keyValue = "";
				String bookID = "";
				bookID = bookEntry.getBookID();
				keyValue = bookEntry.getDataMonth() + "_" + bookEntry.getBookName() + "_" + bookEntry.getPrice() + "_"
						+ bookEntry.getQuantity() + "_" + bookEntry.getBookType() + "_" + bookEntry.getOrigPubPrice()
						+ "_" + bookEntry.getGrowthRate()+"_"+bookEntry.getPublishYear();
				// BookId is the key and a string of Monthweek, price and
				// quantity is the value
				System.out.println("book ID: " + bookID + "keyValue: " + keyValue);
				context.write(new Text(bookID), new Text(keyValue));
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}
}
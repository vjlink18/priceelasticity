package edu.tamu.isys.priceelasticity;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class Mapper2_Forecast extends Mapper<LongWritable, Text, Text, Text> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Convert the line, which is received as a Text object,
     * to a String object.
     */
    String lineNew = value.toString();

	 String[] lineNewSplitArray =lineNew.split(" ") ;
	if(lineNewSplitArray.length >1){
	 
	String mapperKey=lineNewSplitArray[0];
	String mapperValue=lineNewSplitArray[1];
		//System.out.println("key :"+singlegenre+"  value :"+movie+"|"+rating);
		context.write(new Text(mapperKey), new Text(mapperValue));
 
	}
  }
}
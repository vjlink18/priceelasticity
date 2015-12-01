import java.io.IOException;
import java.util.Scanner;
import java.util.*;
import java.lang.*;
import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class ForecastReducer extends Reducer<Text, Text, Text, Text> {

  /*
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives a key of type Text, a set of values of type
   * IntWritable, and a Context object.
   */
  @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		List<String> fullIteratorValues = new ArrayList<String>();
		List<String> fullDatamonthValues = new ArrayList<String>();

		for(Text item : values)
		{
		fullIteratorValues.add(item.toString()); // creates full value [ie DM_P_,,,,] list
		fullDatamonthValues.add(item.toString().split("\\_")[0]); // creates ful movies list (with duplicates)
                //iteratorCount++;
              
		}

		//Sort datasets
		Collections.sort(fullIteratorValues);
		Collections.sort(fullDatamonthValues);


		//find minimum datamonth -> future = year+2/month
		//String minDM = fullDatamonthValues.get(0);
		//System.out.println("----------------Min year " + minDM);
		//for(int i=0; i<fullDatamonthValues.size(); i++)
    		//System.out.println(fullDatamonthValues.get(i));
		//for(int i=0; i<fullIteratorValues.size(); i++)
    		//System.out.println(fullIteratorValues.get(i));
		String newValue="";
		for (int i = 0; i < fullIteratorValues.size(); i++) {
			//System.out.println("In for "+i);
			String[] oldValueSplit = fullIteratorValues.get(i).split("\\_");

			if (i > 11) { //System.out.println("More than 11");
			newValue = oldValueSplit[0] + "_" + oldValueSplit[1] + "_" + oldValueSplit[2] + "_" + oldValueSplit[3] + "_" + oldValueSplit[4] + "_" + oldValueSplit[5] + "_" + oldValueSplit[6] + "_" + oldValueSplit[7] + "_" + oldValueSplit[2] + "_1_2000";
			} else {//System.out.println("Less than 11");
		    newValue = oldValueSplit[0] + "_" + oldValueSplit[1] + "_" + oldValueSplit[2] + "_" + oldValueSplit[3] + "_" + oldValueSplit[4] + "_" + oldValueSplit[5] + "_" + oldValueSplit[6] + "_" + oldValueSplit[7] + "_" + oldValueSplit[2] + "_0_2000";
			} // Chnaged Is_Curr flag

			//System.out.println("New value is "+newValue + "\n");
			context.write(new Text(key),new Text(newValue));	
		}


		//System.out.println(String.format("MSE: %f", TSAError.calculateMSE(y, prediction, period, m, false)));

		int[] baselineValues = new int[24];
		String priceValue = "";
		String oldDM = "";
		String newDM = "";

		for (int i = 0; i < 24; i++) {
			priceValue = fullIteratorValues.get(i).split("_")[2];
			baselineValues[i] = Integer.parseInt(priceValue);
		}

		double alpha = 0.06;

		double beta = 0.98;

		double gamma = 0.48;

		int period = 12;

		int m = 12;


		double[] prediction = ForecastPrediction.forecast(baselineValues, alpha, beta, gamma, period, m, true);
			
			//printArray("Seasonal forecast: ", prediction);
		//System.out.println( "--------------------Predicted values \n");
		//Output for predicted 1 year
		for (int i = 12; i < 24; i++) {
			oldDM = fullDatamonthValues.get(i);
			int year = Integer.parseInt(oldDM.substring(0, 4));
			year = year + 1;
			//System.out.println("year" + year);
			String month = oldDM.substring(4, 6);
			//System.out.println("month" + month);
			newDM = Integer.toString(year) + month;
			//System.out.println("neew dtamonth" + newDM);
			String[] oldValueSplit = fullIteratorValues.get(i).split("_");
			newValue = newDM + "_" + oldValueSplit[1] + "_" + oldValueSplit[2] + "_" + oldValueSplit[3] + "_" + oldValueSplit[4] + "_" + oldValueSplit[5] + "_" + oldValueSplit[6] + "_" + oldValueSplit[7] + "_" + prediction[12 + i] + "_0_2000";
			//System.out.println(newValue + "\n");
			context.write(new Text(key),new Text(newValue));	
		}

		


	}



}
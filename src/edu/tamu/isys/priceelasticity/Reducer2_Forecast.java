package edu.tamu.isys.priceelasticity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2_Forecast extends Reducer <Text, Text, Text, Text> 
{

	@Override
	public void reduce(Text key, Iterable < Text > values, Context context)
	throws IOException, InterruptedException 
	{
		List <String> fullIteratorValues = new ArrayList <String>();
		List <String> fullDatamonthValues = new ArrayList <String>();

		for (Text item: values) 
		{
			fullIteratorValues.add(item.toString()); // creates full value [ie DM_P_,,,,] list
			fullDatamonthValues.add(item.toString().split("\\_")[0]); // creates ful movies list (with duplicates)
		}

		//Sort datasets
		Collections.sort(fullIteratorValues);
		Collections.sort(fullDatamonthValues);

		String newValue = "";
		for (int i = 0; i < fullIteratorValues.size(); i++) 
		{
			String[] oldValueSplit = fullIteratorValues.get(i).split("\\_");

			if (i > 11) 
			{ 
				newValue = oldValueSplit[0] + "_" + oldValueSplit[1] + "_" + oldValueSplit[2] + "_" + oldValueSplit[3] + "_" + oldValueSplit[4] + "_" + oldValueSplit[5] + "_" + oldValueSplit[6] + "_" + oldValueSplit[7] + "_" + oldValueSplit[2] + "_1_" + oldValueSplit[8];
			} 
			else 
			{ 
				newValue = oldValueSplit[0] + "_" + oldValueSplit[1] + "_" + oldValueSplit[2] + "_" + oldValueSplit[3] + "_" + oldValueSplit[4] + "_" + oldValueSplit[5] + "_" + oldValueSplit[6] + "_" + oldValueSplit[7] + "_" + oldValueSplit[2] + "_0_" + oldValueSplit[8];
			} 

			context.write(new Text(key), new Text(newValue));
		}

		int[] baselineValues = new int[24];
		String priceValue = "";
		String oldDM = "";
		String newDM = "";

		for (int i = 0; i < 24; i++) 
		{
			priceValue = fullIteratorValues.get(i).split("_")[2];
			baselineValues[i] = (int)(Double.parseDouble(priceValue) + 0.5);
		}

		//Setting parameteric values as per forecasting algorithm & prior research
		double alpha = 0.06;
		double beta = 0.98;
		double gamma = 0.48;
		int period = 12;
		int m = 12;

		double[] prediction = ForecastPrediction.forecast(baselineValues, alpha, beta, gamma, period, m, true);

		for (int i = 12; i < 24; i++) 
		{
			oldDM = fullDatamonthValues.get(i);
			int year = Integer.parseInt(oldDM.substring(0, 4));
			year = year + 1;
			String month = oldDM.substring(4, 6);
			newDM = Integer.toString(year) + month;
			String[] oldValueSplit = fullIteratorValues.get(i).split("_");
			newValue = newDM + "_" + oldValueSplit[1] + "_" + oldValueSplit[2] + "_" + oldValueSplit[3] + "_" + oldValueSplit[4] + "_" + oldValueSplit[5] + "_" + oldValueSplit[6] + "_" + oldValueSplit[7] + "_" + prediction[12 + i] + "_0_" + oldValueSplit[8];
			context.write(new Text(key), new Text(newValue));
		}
	}
}
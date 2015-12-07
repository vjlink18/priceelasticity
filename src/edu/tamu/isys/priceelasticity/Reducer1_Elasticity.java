package edu.tamu.isys.priceelasticity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1_Elasticity extends Reducer <Text, Text, Text, Text> 
{
	Double eta;
	// Assuming qStart and pStart as zero initially
	double qStart = 0.0, pStart = 0.0;
	double qEnd, pEnd, deltaQ, deltaP;
	String finalKeyVal = "";

	@Override
	public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException 
	{
		List <String> fullIteratorValues = new ArrayList <String> ();
		for (Text item: values) 
		{
			// creates full value [ie DM_P_,,,,] list duplicates
			fullIteratorValues.add(item.toString()); 
		}
		
		// Sort datasets
		Collections.sort(fullIteratorValues);
		for (int i = 0; i < fullIteratorValues.size(); i++) 
		{
			String[] oldValueSplit = fullIteratorValues.get(i).split("_");

			pEnd = Double.parseDouble(oldValueSplit[2]);
			qEnd = Double.parseDouble(oldValueSplit[3]);

			deltaQ = (qStart - qEnd) / (qStart + qEnd);
			deltaP = (pStart - pEnd) / (pStart + pEnd);
			eta = deltaQ / deltaP;
			eta = Math.abs(eta);

			pStart = pEnd;
			qStart = qEnd;

			finalKeyVal = oldValueSplit[0] + "_" + oldValueSplit[1] + "_" + oldValueSplit[2] + "_" + oldValueSplit[3] + "_" + oldValueSplit[4] + "_" + oldValueSplit[5] + "_" + oldValueSplit[6] + "_" + eta + "_" + oldValueSplit[7];
			context.write(new Text(key), new Text(finalKeyVal));
		}
	}
}
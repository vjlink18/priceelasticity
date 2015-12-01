package edu.tamu.isys.priceelasticity;

/* Imports have been organized for RatingsReducer Class */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1_Elasticity extends Reducer<Text, Text, Text, Text> {
	/*
	 * For improving efficiency of the program, variables have been declared
	 * outside the methods to optimize the processing
	 */
	Double eta;
	// Assuming qStart and pStart as zero initially
	double qStart = 0.0, pStart = 0.0;
	double qEnd, pEnd, deltaQ, deltaP;
	String finalKeyVal = "";

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> fullIteratorValues = new ArrayList<String>();

		for (Text item : values) {
			fullIteratorValues.add(item.toString()); // creates full value [ie
														// DM_P_,,,,] list //
														// duplicates)
		}
		// Sort datasets
		Collections.sort(fullIteratorValues);
		for (int i = 0; i < fullIteratorValues.size(); i++) {
			// System.out.println("In for "+i);
			String[] oldValueSplit = fullIteratorValues.get(i).split("_");

			pEnd = Double.parseDouble(oldValueSplit[2]);
			qEnd = Double.parseDouble(oldValueSplit[3]);

			System.out.println("pEnd: " + pEnd + "qEnd: " + qEnd);

			System.out.println("pStart: " + pStart + "qStart: " + qStart);

			deltaQ = (qStart - qEnd) / (qStart + qEnd);
			System.out.println("deltaQ: " + deltaQ);
			System.out.println("pStart: " + pStart + "pEnd: " + pEnd);
			deltaP = (pStart - pEnd) / (pStart + pEnd);
			System.out.println("deltaP: " + deltaP);
			eta = deltaQ / deltaP;
			System.out.println("eta: " + eta);
			eta = Math.abs(eta);
			System.out.println("New eta: " + eta);
			pStart = pEnd;
			qStart = qEnd;
			System.out.println("pStart New: " + pStart + "qStart" + qStart);
			finalKeyVal = oldValueSplit[0] + "_" + oldValueSplit[1] + "_" + oldValueSplit[2] + "_" + oldValueSplit[3]
					+ "_" + oldValueSplit[4] + "_" + oldValueSplit[5] + "_" + oldValueSplit[6] + "_" + eta+"_"+oldValueSplit[7];

			System.out.println("final key value: " + finalKeyVal);

			context.write(new Text(key), new Text(finalKeyVal));
		}
	}
}
package edu.tamu.isys.priceelasticity;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper2_Forecast extends Mapper < LongWritable, Text, Text, Text > {

	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
		//Input is received in format below
		//B [space] DM[0]_b[1]_p[2]_q[3]_bt[4]_opp[5]_GR[6]_ e[7]_publishYear[8]
		/*
		 * Convert the line, which is received as a Text object,
		 * to a String object.
		 */
		String lineNew = value.toString();
		String[] lineNewSplitArray = lineNew.split("\\t");
		if (lineNewSplitArray.length > 1) {

			String mapperKey = lineNewSplitArray[0];
			String mapperValue = lineNewSplitArray[1];

			System.out.println("Passing " + mapperKey + "-->" + mapperValue);
			context.write(new Text(mapperKey), new Text(mapperValue));

		}
	}
}
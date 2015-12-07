package edu.tamu.isys.priceelasticity;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3_IndvPrices extends Reducer<Text, Text, Text, Text> 
{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text val : values) {
			//parse values from input string to extract various attributes
			String line = val.toString();

			String[] parts = line.split("_");
		
			//DataMonth of the record
			String DM = parts[0];
			String b = parts[1];
			String pStr = parts[2];
			double p = Double.parseDouble(pStr);
			String qStr = parts[3];
			int q = Integer.parseInt(qStr);
			String bt = parts[4];
			String oppStr = parts[5];
			double opp = Double.parseDouble(oppStr);
			String GRStr = parts[6];
			double GR = Double.parseDouble(GRStr);
			String eStr = parts[7];
			double e = Double.parseDouble(eStr);
			String p1Str = parts[8];
			double p1 = Double.parseDouble(p1Str);
			String boolCStr = parts[9];
			int boolC = Integer.parseInt(boolCStr);
			String PublishYearStr = parts[10];

			double PublishYear = Double.parseDouble(PublishYearStr);

			String dmYearStr = DM.substring(0, 4);
			double dmYear = Double.parseDouble(dmYearStr);

			double P12 = 0.0;
			double P3 = 0.0;

			// Price elasticity P12 = (((1+GrowthRate)*(ActualPrice/2)*(elasticity/(1+elasticity))*Is_current) + (((1+ GrowthRate)*(PredictedPrice/2)*(elasticity/(1+elasticity))*(1-Is_current) ) )

			P12 = (((1 + GR) * (p / 2) * (e / (1 + e)) * boolC) + ((1 + GR)
					* (p1 / 2) * (e / (1 + e)) * (1 - boolC)));
			
			// Price Depriciation P3= OriginalPublishPrice(1- ((CurrentYear-PublishYear)/Age))

			
			P3 = opp * (1 - ((dmYear - PublishYear) / 10));
			
			//If Depreciation price is less than half of original price of book then set price to half of original publish price 
			if (Math.abs(opp - P3) < opp/2)
			{
				P3=opp/2;
			}
			
			//Appending two new values to the original set of values
			String bookPrices = line + "_" + P12 + "_" + P3;
			
			context.write(key, new Text(bookPrices));
			
		}

	}
}
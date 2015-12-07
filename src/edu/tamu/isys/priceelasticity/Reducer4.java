package edu.tamu.isys.priceelasticity;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.awt.Desktop;
import java.net.URI;

public class Reducer4 extends Reducer<Text, Text, Text, Text> 
{
	/* For improving efficiency of the program, variables have been declared outside the methods to optimize the processing */
	private String lineText="";
	private String parts[];
	private Double idealPrice=0d;
	private String dataMonth="";
	private String book="";
	private String originalPrice="";
	private Text newValue;
	private String price12="";
	private String price3="";
	private String newValueString="";
	private String bookType="";
	private String quantity="";
	private String price="";
	private String growthRate="";
	private String elasticityETA="";
	private String forecastedPrice="";
	private String isCurrent="";
	private String yearOfPublishing="";
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		//Assigning weights to compute prices
		double W1=0.7;
		double W2=0.3;
		
		for(Text value : values)
		{
			lineText = value.toString();
			parts = lineText.split("\\_");
			
			//Parsing raw input into variables
			dataMonth=parts[0];
			book=parts[1];
			price=parts[2];
			quantity=parts[3];
			bookType=parts[4];
			originalPrice=parts[5];
			growthRate=parts[6];
			elasticityETA=parts[7];
			forecastedPrice=parts[8];
			isCurrent=parts[9];
			yearOfPublishing=parts[10];
			price12=parts[11];
			price3=parts[12];
			
			idealPrice= W1*Double.parseDouble(price12) + W2*Double.parseDouble(price3);
			newValueString=dataMonth+"_"+book+"_"+price+"_"+quantity+"_"+bookType+"_"+originalPrice+"_"+growthRate+"_"+elasticityETA+"_"+forecastedPrice+"_"+isCurrent+"_"+yearOfPublishing+"_"+price12+"_"+price3+"_"+W1+"_"+W2+"_"+idealPrice.toString();
			newValue=new Text(newValueString);
			context.write(key, newValue);
		}
		try
		{
			if(Desktop.isDesktopSupported())
			{
			  Desktop.getDesktop().browse(new URI("http://priceelasticity.azurewebsites.net/"));
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}
}
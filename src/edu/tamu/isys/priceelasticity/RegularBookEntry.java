package edu.tamu.isys.priceelasticity;

import java.text.ParseException;

public class RegularBookEntry extends BookEntry {
	private String authors="";
	
	public RegularBookEntry(String rawData) throws ParseException {
		super(rawData);
	}
	
	public String getAuthor()
	{
		return authors;
	}
	
	public String[] getAuthors()
	{
		int loop_var;
		String[] authorList = authors.split(",");
		
		/* Code optimized for efficiency by using ++loop_var to speed up for loop processing*/
		for(loop_var=0; loop_var < authorList.length; ++loop_var)
		{
			authorList[loop_var] = authorList[loop_var].trim();
		}
		return authorList;
	}
}

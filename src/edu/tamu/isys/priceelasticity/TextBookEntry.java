package edu.tamu.isys.priceelasticity;

import java.text.ParseException;

public class TextBookEntry extends BookEntry {
	private String edition="";
	
	public TextBookEntry(String rawData) throws ParseException {
		super(rawData);
		// TODO Auto-generated constructor stub
	}
	
	public String getEdition()
	{
		return edition;
	}
}

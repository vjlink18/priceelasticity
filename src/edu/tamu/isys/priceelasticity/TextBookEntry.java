package edu.tamu.isys.priceelasticity;
/* This class is used for text book entry data 

*/
import java.text.ParseException;

public class TextBookEntry extends BookEntry {
	private String edition = "";

	public TextBookEntry(String rawData) throws ParseException {
		super(rawData);
	}

	public String getEdition() {
		return edition;
	}
}

package edu.tamu.isys.priceelasticity;

import java.text.ParseException;

public class BookEntry {
	
	private String ISBN= "";
	private String title= "";
	private String publisher= "";
	private String publishingDate= "";
	private String price= "";
	private String errorMessage = "";
	private String authors="";
	
	public BookEntry (String rawData) throws ParseException
	{
		try
		{
			String[] parts = rawData.split("::");
			this.ISBN = parts[0].trim();
			this.title=parts[1].trim();
			this.publisher=parts[3].trim();
			this.publishingDate=parts[4].trim();
			this.price=parts[5].trim();
		}
		catch (Exception e)
		{
			errorMessage = e.getStackTrace().toString()+":"+rawData;
			System.out.println(errorMessage);
		}
	}
	
	/* Method to return Book ISBN */
	public String getISBN()
	{
		return ISBN;
	}
	
	/* Method to return Book Title */
	public String getTitle()
	{
		return title;
	}
	
	/* Method to return Book Publisher*/
	public String getPublisher()
	{
		return publisher;
	}
	
	/* Method to return Book Publishing Date */
	public String getPublishingDate()
	{
		return publishingDate;
	}
	
	/* Method to return Book Price */
	public String getPrice()
	{
		return price;
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
	
	/* Method to return Error Message as string */
	public String getError()
	{
		return errorMessage;
	}
	
	/* Method to return the status of Error Message Code */
	public boolean hasError()
	{
		return !errorMessage.isEmpty();
	}
}

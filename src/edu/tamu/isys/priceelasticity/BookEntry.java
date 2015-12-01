package edu.tamu.isys.priceelasticity;

import java.text.ParseException;

public class BookEntry {

	private String dataMonth = "";
	private String bookID = "";
	private String bookName = "";
	private String price = "";
	private String quantity = "";
	private String bookType = "";
	private String origPubPrice = "";
	private String growthRate = "";

	private String errorMessage = "";

	public BookEntry(String rawData) throws ParseException {
		try {

			String[] parts = rawData.split("::");
			this.dataMonth = parts[0].trim();
			this.bookID = parts[1].trim();
			this.bookName = parts[2].trim();
			this.price = parts[3].trim();
			this.quantity = parts[4].trim();
			this.bookType = parts[5].trim();
			this.origPubPrice = parts[6].trim();
			this.growthRate = parts[7].trim();
		} catch (Exception e) {
			errorMessage = e.getStackTrace().toString() + ":" + rawData;
			System.out.println(errorMessage);
		}
	}

	/* Method to return Book DataMonth */
	public String getDataMonth() {
		return dataMonth;
	}

	/* Method to return Book ID */
	public String getBookID() {
		return bookID;
	}

	/* Method to return Book Name */
	public String getBookName() {
		return bookName;
	}

	/* Method to return Book Price */
	public String getPrice() {
		return price;
	}

	/* Method to return Book Quantity */
	public String getQuantity() {
		return quantity;
	}

	/* Method to return Book Type */
	public String getBookType() {
		return bookType;
	}

	/* Method to return Book Original Publisher Price */
	public String getOrigPubPrice() {
		return origPubPrice;
	}

	/* Method to return Book Growth Rate */
	public String getGrowthRate() {
		return growthRate;
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
	public String getError() {
		return errorMessage;
	}

	/* Method to return the status of Error Message Code */
	public boolean hasError() {
		return !errorMessage.isEmpty();
	}
}

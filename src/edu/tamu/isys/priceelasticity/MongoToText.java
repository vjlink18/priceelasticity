package edu.tamu.isys.priceelasticity;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;


public class MongoToText 
{
	public static void main(final String[] args) throws IOException 
	{
		File file = new File("output.txt");
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);

		//Connect to MongoDB on the server to retrieve book related data 
		MongoClientURI connectionString = new MongoClientURI("mongodb://vikramj:vikram@ds054288.mongolab.com:54288/priceelasticitydb");
		MongoClient mongoClient = new MongoClient(connectionString);

		MongoDatabase database = mongoClient.getDatabase("priceelasticitydb");
		FindIterable <Document> iterable = database.getCollection("MapperInput").find();

		//Write all the output to text file which will work as input file for Mapper
		for (Document d: iterable) 
		{
			System.out.println("Printing " + d.toString());
			String DM = d.get("DM").toString();
			String BookID = d.get("BookID").toString();
			String b = d.get("b").toString();
			String p = d.get("p").toString();
			String q = d.get("q").toString();
			String bt = d.get("bt").toString();
			String opp = d.get("opp").toString();
			String GR = d.get("GR").toString();
			String publishYear = d.get("publishYear").toString();

			String mapperInput = DM + "::" + BookID + "::" + b + "::" + p + "::" + q + "::" + bt + "::" + opp + "::" + GR + "::" + publishYear;

			//Write output to file
			bw.write(mapperInput + "\n");
		}
		/*Connections closed to improve efficiency*/
		bw.close();
		mongoClient.close();
	}
}
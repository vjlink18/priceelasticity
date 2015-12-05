package edu.tamu.isys.priceelasticity;

/* Imports have been organised for Program Class */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Program extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		/* Console message for program initiation */
		System.out.println("Program initializing");
		
		int res = ToolRunner.run(new Configuration(), new Program(), args);
		
		/* System exits after posting console message of exit code informing user of the outcome of program*/ 
		System.out.println("Program ending with exit code: "+res);
		System.exit(res);
	}
	public int run (String args[]) throws Exception
	{
		/* Uncomment when four jobs are ready
		String OUTPUT1="output_mapreduce1";
		String OUTPUT2="output_mapreduce2";
		String OUTPUT3="output_mapreduce3";
		*/

		/* Job 1 properties */
		Job job1 = Job.getInstance(getConf(), "job 1");
		job1.setJarByClass(Program.class);
		
		job1.setMapperClass(Mapper1_Elasticity.class);
		job1.setReducerClass(Reducer1_Elasticity.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		/* Uncomment when multiple jobs running
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT1));
		job1.waitForCompletion(true);
		*/

		return job1.waitForCompletion(true) ? 0 : 1;
		/* Job 1 properties end*/

		/* Commented to run for one job at a time 

		/* Job 2 Properties 
		Job job2 = Job.getInstance(getConf(), "job 2");
		job2.setJarByClass(Program.class);
		
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(OUTPUT1);
		FileOutputFormat.setOutputPath(job2, new Path(OUTPUT2));
		job2.waitForCompletion(true);
		
		/* Job 2 properties end 

		/* Job 3 properties 
		Job job3 = Job.getInstance(getConf(), "job 3");
		job3.setJarByClass(Program.class);
		
		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer3.class);
		
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(OUTPUT2);
		FileOutputFormat.setOutputPath(job3, new Path(OUTPUT3));
		job3.waitForCompletion(true);
		
		/* Job 3 properties end

		/* Job 4 properties 
		Job job4 = Job.getInstance(getConf(), "job 4");
		job4.setJarByClass(Program.class);
		
		job4.setMapperClass(Mapper4.class);
		job4.setReducerClass(Reducer4.class);
		
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
		
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job4, new Path(OUTPUT3);
		FileOutputFormat.setOutputPath(job4, new Path(args[1]));		

		return job4.waitForCompletion(true) ? 0 : 1;
		/* Job 4 properties end*/
	}
}
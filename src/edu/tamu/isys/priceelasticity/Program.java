package edu.tamu.isys.priceelasticity;

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

public class Program extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		/* Console message for program initiation */
		System.out.println("Program initializing");
		int res = ToolRunner.run(new Configuration(), new Program(), args);

		/* System exits after posting exit code */
		System.out.println("Program ending with exit code: " + res);
		System.exit(res);
	}

	@Override
	public int run(String args[]) throws Exception {

		String OUTPUT1 = "output_mapreduce1";
		String OUTPUT2 = "output_mapreduce2";
		String OUTPUT3 = "output_mapreduce3";

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
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT1));
		job1.waitForCompletion(true);
		/* Job 1 properties end */

		/* Job 2 properties */
		Job job2 = Job.getInstance(getConf(), "job 2");
		job2.setJarByClass(Program.class);

		job2.setMapperClass(Mapper2_Forecast.class);
		job2.setReducerClass(Reducer2_Forecast.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(OUTPUT1));
		FileOutputFormat.setOutputPath(job2, new Path(OUTPUT2));
		job2.waitForCompletion(true);
		/* Job 2 properties end */

		
		/* Job 3 properties */
		Job job3 = Job.getInstance(getConf(), "job 3");
		job3.setJarByClass(Program.class);

		job3.setMapperClass(Mapper3_IndvPrices.class);
		job3.setReducerClass(Reducer3_IndvPrices.class);

		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(OUTPUT2));
		FileOutputFormat.setOutputPath(job3, new Path(OUTPUT3));
		job3.waitForCompletion(true);
		/* Job 3 properties end */


		/* Job 4 properties */
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

		FileInputFormat.addInputPath(job4, new Path(OUTPUT3));
		FileOutputFormat.setOutputPath(job4, new Path(args[1]));
		return job4.waitForCompletion(true) ? 0 : 1;
		/* Job 4 properties end */
	}
}
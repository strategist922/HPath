/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package br.ufpr.inf.hpath;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HPath {
	
	/**
	 *  
	 * @author Edson
	 *
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
//		private Text tmp = new Text();
		String query;
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
//			int begin;
			String item = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(item);
			word.set(tokenizer.nextToken());
			System.out.println(query);
			
//			if (query.matches("//.*")) {
				context.write(word, one);
//				begin = value.toString().indexOf("\n");
//			} else if (query.matchs(begin)) {
//				tmp.set(value.toString().substring(begin + 1));
//				context.write(value, one);
//			} 
		}

		public void configure(@SuppressWarnings("deprecation") JobConf job) {
			query = job.get("xpath.query");
		}
	}
	
	/**
	 * 
	 * @author Edson
	 *
	 */
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) 
				sum += val.get();
//			/* TODO: Validate attributes */
//			if (checkAttr(key)) {
			context.write(key, new IntWritable(sum));
//			}
		}
		
//		public boolean checkAttr (Text value) {
//			String[] queryItems = xPathQuery.toString().split("/");
//			String[] valueItems = value.toString().split("\t");
//			String item = valueItems[1];
//			return true;
//		}
	}
	
	/**
	 * Execute the XPath query as a Hadoop job
	 * @param xpath_query XPath query submitted by the user via cli.
	 * @param inputFile XML file which has all data.
	 * @param outputFile Query's result is stored in this file. 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		if (args.length < 1) {
			System.out.println("USAGE: hpath [xpath_query] [input_file] [<output_dir>]");
			System.exit(-1);
		}
		
		System.out.println("***************");
		System.out.println(" Query  -> " + args[2]);
		System.out.println(" Input  -> " + args[0]);
		System.out.println(" Output -> " + args[1]);	
		System.out.println("***************");
		
		String xpath_query = args[2];
		String inputFile = args[0];
		String outputFile = args[1];
		String tag = "";
				
		// tag = getFisrtQueryTag(xpath_query);
		tag = getLastQueryTag(xpath_query);
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<" + tag);
		conf.set("xmlinput.end", "</" + tag + ">");
		conf.set("xpath.query", xpath_query);

		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HPath");
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(inputFile);
		Path outFile = new Path(outputFile);
		
		if (!fs.exists(inFile)) {
			System.out.println("error: Input file not found.");
			System.exit(-1);
		}
		if (!fs.isFile(inFile)) {
			System.out.println("error: Input should be a file.");
			System.exit(-1);
		}
		if (fs.exists(outFile)) {
			System.out.println("error: Output already exists.");
			System.exit(-1);
		}
		
		job.setJarByClass(HPath.class);
	
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(XmlItemInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inFile);
		FileOutputFormat.setOutputPath(job, outFile);
		job.waitForCompletion(true);
	}

	/**
	 * This is used to split the file in blocks.
	 * @param xpath_query 
	 * @return firstTag 
	 */
	@SuppressWarnings("unused")
	private String getFisrtQueryTag(String xpath_query) {
		String tags[];
		tags = xpath_query.split("/");
		if (tags.length < 1)  
			return null;
		return (tags[1].isEmpty() ? tags[2] : tags[1]);		
	}
	
	/**
	 * This is used to split the file in blocks.
	 * @param xpath_query 
	 * @return firstTag 
	 */
	private static String getLastQueryTag(String xpath_query) {
		String tags[];
		tags = xpath_query.split("/");
		if (tags.length < 1)  
			return null;
		return tags[tags.length-1];		
	}
}
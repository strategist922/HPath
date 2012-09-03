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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Executor {
	private static String xPathQuery;
	
	/**
	 *  
	 * @author Edson
	 *
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text tmp = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int begin;
			String item = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(item);
			word.set(tokenizer.nextToken());
			if (xPathQuery.matches("//.*")) {
				context.write(value, one);
			} else if (word.toString().equals(xPathQuery)) {
				begin = value.toString().indexOf("\t");
				tmp.set(value.toString().substring(begin + 1));
				context.write(value, one);
			} 
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
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	/**
	 * Execute the XPath query as a Hadoop job
	 * @param xpath_query XPath query submitted by the user via cli.
	 * @param inputFile XML file which has all data.
	 * @param outputFile Query's result is stored in this file. 
	 * @throws Exception
	 */
	public void runQuery(String xpath_query, String inputFile, String outputFile)
			throws Exception {
		long now = System.nanoTime();
		String tag = "";
		xPathQuery = xpath_query;
		// tag = getFisrtQueryTag(xpath_query);
		tag = getLastQueryTag(xpath_query);
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<" + tag);
		conf.set("xmlinput.end", "</" + tag + ">");

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HPath");

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(XmlItemInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputFile + "_" + now));
		job.waitForCompletion(true);
		xPathQuery = "";
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
	private String getLastQueryTag(String xpath_query) {
		String tags[];
		tags = xpath_query.split("/");
		if (tags.length < 1)  
			return null;
		return tags[tags.length-1];		
	}
}
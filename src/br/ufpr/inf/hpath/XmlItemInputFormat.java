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

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlItemInputFormat extends TextInputFormat {

	private static final Logger log = LoggerFactory
			.getLogger(XmlItemInputFormat.class);

	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		try {
			return new XmlRecordReader((FileSplit) split,
					context.getConfiguration());
		} catch (IOException ioe) {
			log.warn("Error while creating XmlRecordReader", ioe);
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * XMLRecordReader class to read through a given xml document to output xml
	 * blocks as records as specified by the start tag and end tag
	 * 
	 */
	public static class XmlRecordReader extends
			RecordReader<LongWritable, Text> {

		private final byte[] startTag;
		private final byte[] endTag;
		private final long start;
		private final long end;
		private final FSDataInputStream fsin;
		private final DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable currentKey;
		private Text currentValue;
		private Path file = null;
		private static int globalTagCursor;
		
		public XmlRecordReader(FileSplit split, Configuration conf)
				throws IOException {
			startTag = conf.get(START_TAG_KEY).getBytes(Charsets.UTF_8);
			endTag = conf.get(END_TAG_KEY).getBytes(Charsets.UTF_8);

			// open the file and seek to the start of the split
			start = split.getStart();
			end = start + split.getLength();
			file = split.getPath();
			FileSystem fs = file.getFileSystem(conf);
			fsin = fs.open(split.getPath());
			fsin.seek(start);
		}

		/**
		 * TODO: The fisrt line must be the xml item path -
		 * /item01/item02/item03 This path will be used to compare the items in
		 * map functions
		 * 
		 * @param key
		 * @param value
		 * @return
		 * @throws IOException
		 * @throws ParserConfigurationException 
		 * @throws SAXException 
		 */
		private boolean next(LongWritable key, Text value) throws IOException, SAXException, ParserConfigurationException {
			buffer.writeBytes(tagPath(startTag));
			buffer.writeBytes("\t");
			if (fsin.getPos() < end && readUntilMatch(startTag, false)) {
				try {
					buffer.write(startTag);
					if (readUntilMatch(endTag, true)) {
						key.set(fsin.getPos());
						value.set(buffer.getData(), 0, buffer.getLength());
						return true;
					}
				} finally {
					buffer.reset();
				}
			}
			return false;
		}

		private String tagPath(byte[] startTag) {
			Document doc = null;
			Element tagElement;
			File f = new File(file.toUri().getPath());
			try {
		           DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		           DocumentBuilder docBuilder = dbf.newDocumentBuilder();
		           doc = docBuilder.parse(f);
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			NodeList lista = doc.getElementsByTagName("*");
			Element root = doc.getDocumentElement();
			String path = "";

			for (int i = globalTagCursor; i < lista.getLength(); i++) {
				tagElement = (Element) lista.item(i);
				String strTag = new String(startTag);
				strTag = strTag.substring(strTag.indexOf("<")+1);
				if (tagElement.getNodeName().equals(strTag)) {
					path = "/" + tagElement.getNodeName().toString();
					while (tagElement.getNodeName() != root.getNodeName()) {
						tagElement = (Element) tagElement.getParentNode();
						path = "/" + tagElement.getNodeName().toString() + path;
					}
					/* This is necessary to move to the next tags */
					globalTagCursor = i + 1;
					break;
				}
			}
			
			if (globalTagCursor >= lista.getLength())
				globalTagCursor = 0;
			return path;
		}

		@Override
		public void close() throws IOException {
			Closeables.closeQuietly(fsin);
		}

		@Override
		public float getProgress() throws IOException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock)
				throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1) {
					return false;
				}
				// save to buffer:
				if (withinBlock) {
					buffer.write(b);
				}

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length) {
						return true;
					}
				} else {
					i = 0;
				}
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && fsin.getPos() >= end) {
					return false;
				}
			}
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return currentKey;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return currentValue;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			currentKey = new LongWritable();
			currentValue = new Text();
			try {
				return next(currentKey, currentValue);
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			}
			return false;
		}
	}
}

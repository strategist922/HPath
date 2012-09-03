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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.xml.sax.SAXException;

public class HPath {
	
	public static int returnIndex(String toIndex) {
		String[] commands = {"", "quit", "help", "load", "saveto", "list"};
		for (int i=0; i<commands.length; i++) {  
	        if (toIndex.equals(commands[i] ) )   
	            return i;  
	    }  
	    return -1;  
    }  
	/**
	 * @param args
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws XPathExpressionException 
	 */
   public static void main (String[] args) 
      throws XPathExpressionException, ParserConfigurationException, SAXException, IOException {
      Executor executor = new Executor();
      Integer keepGoing = 1;
      String inputFileName = new String();
      String outputFileName = new String();
      double startTime;
      
      do {
         System.out.print("hpath >> ");
         BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));   
         try {
            String[] commands;
         	commands = buffer.readLine().split(" ");
         	switch (returnIndex(commands[0])) {  
	         	case 0 : // do nothing 
	         		break;	
	         	case 1 : // quit 
	         		keepGoing = 0; 
	         		break;
	         	case 2 : // help
	         		System.out.println("help \t- Show this help menu.");
	         		System.out.println("quit \t- Quit hpath-cli.");
	         		System.out.println("load \t- load input file.");
	         		System.out.println("saveto \t- load input file.");
	         		System.out.println("/xpath \t- XPath query. Type your query direct in the cli.");
	         		// System.out.println("list - list current files in use.");
	         		break;
	         	case 3 : // set input file
	         		System.out.print("XML file path: ");
	             	inputFileName = buffer.readLine();
	             	File in = new File(inputFileName);
	             	if (in.exists())
	             		System.out.println(" @ " + inputFileName);
	             	else
	             		System.out.println(" File not found.");
	             	break;
	         	case 4 : // set output file
	         		System.out.print("Save to: ");
	             	outputFileName = buffer.readLine();
	             	File out = new File(outputFileName);
	             	if (out.exists())
	             		System.out.println(" This file already exists.");
	             	else
	             		System.out.println(" # " + outputFileName);
	             	break;
	             	
	         	case 5 : // list
	         		System.out.println(" This feature is not enabled.");
	         		break;
	         	default: 
	         		if (inputFileName.isEmpty()) {
	         			System.out.println(" *** You need to select a XML file. Try 'load'. ");
	         		} else {
	         			startTime = (double)System.nanoTime();
		         		if (outputFileName.isEmpty())
		         			executor.runQuery(commands[0], inputFileName, "output/result");
		         		else
		         			executor.runQuery(commands[0], inputFileName, outputFileName);
		         		
	         			System.out.println(" @ " + inputFileName);
		         		startTime = ((double)System.nanoTime() - startTime) / 1000000000.0;
		         		System.out.println("Hey, it took " + startTime + " seconds to run.");
	         		}
         	}  
         } catch (IOException ioe) {
         	System.out.println("Error: " + ioe.getMessage());
         } catch (Exception e) {
			e.printStackTrace();
		}
      } while (keepGoing.equals(1)); 
      System.out.println("Bye.");
   }
}
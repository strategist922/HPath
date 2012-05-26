package br.ufpr.inf.hpath;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.xml.sax.SAXException;

public class HPath {
	
	public static int returnIndex(String toIndex) {
		String[] commands = {"quit","load",""};
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
      String xmlFileName = new String();
      
      do {
         System.out.print("hpath >> ");
         BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));   
         try {
            String[] commands;
         	commands = buffer.readLine().split(" ");
         	switch (returnIndex(commands[0])) {  
	         	case 0 : keepGoing = 0; break;
	         	case 1 : 
	         		System.out.print("File name: ");
	             	xmlFileName = buffer.readLine();
	             	System.out.println(" @ " + xmlFileName);
	         	case 2 : break;
	         	default: 
	         		if (xmlFileName.isEmpty()) {
	         			System.out.println(" *** You need to select a XML file. Try 'load'. ");
	         		} else {
	         			executor.runQuery(commands[0], xmlFileName);
	         		}
         	}  
         } catch (IOException ioe) {
         	System.out.println("Error: " + ioe.getMessage());
         }
      } while (keepGoing.equals(1)); 
      System.out.println("Bye.");
   }
}

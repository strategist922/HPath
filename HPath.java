import java.io.*;

public class HPath {
   public static void main (String[] args) {
      String command = null;
      do {
         System.out.print("hpath >> ");
         BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
         
         try {
         	command = buffer.readLine();
         } catch (IOException ioe) {
         	System.out.println("IO error: Could not read you command!");
         	System.exit(1);
         }
         
         if(!command.equals("") && !command.equals("quit")) {
            System.out.println(" ***  , " + command);
         } 
      } while (!command.equals("quit")); 
      System.out.println("Bye.");
   }
}

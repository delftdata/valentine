package core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  final private static Logger LOG =
      LoggerFactory.getLogger(Utils.class.getName());

  public static long computeAttrId(String dbName, String sourceName, String columnName) {
	  CRC32 crc = new CRC32();
	  String s = dbName.concat(sourceName).concat(columnName);
	  crc.update(s.getBytes());
	  long id = crc.getValue();
	  return id;
//    String sourceAndField = sourceName.concat(columnName);
//    String all = dbName.concat(sourceAndField);
//    return all.hashCode();
  }

  	public static void appendLineToFile(File errorLogFile, String msg) {
  		try {
  			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(errorLogFile, true)));
  			out.println(msg);
  			out.close();
  		} 
  		catch (IOException io) {
  			io.printStackTrace();
  			LOG.warn("Error log could not be written to error log file");
  		}
  	}
}

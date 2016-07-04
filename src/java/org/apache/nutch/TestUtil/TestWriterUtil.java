package org.apache.nutch.TestUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.nutch.protocol.Content;


public abstract class TestWriterUtil {

	private static BufferedWriter out = null;
	
	public static void write(Content data, String outfile) {
		
		write(data.toString(), outfile);
	}
	
	public static void write(String data, String outfile) {
		

		try {
			new File("outfile").delete();
		    out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outfile),"UTF-8"));
			out.write(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

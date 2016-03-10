package de.hhu.bsinfo.dxram.run.nothaas;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class Sandbox {

	public static void main(String[] args)
	{		
		String text = new String("25,6418,8233,176,33695,8233,6418,6418,176,6418,8233,30218,30218,33695,33695,176,176,176,30218,18716,33695,30218,6418,12024,12024,18716,33695,30218,18716,8233,6418,8233,30218,12024,176,18716,33695,176,18716,8233,33695,18716,30218,8233,25,176,25,18716,25,30218,25,30218,12024,12024,18716,25,18716,12024,12024,8233,30218,8233,25,33695,33695,25,8233,12024,30218,33695,8233,6418,6418,6418,25,176,176,12024,12024,176,25,12024,25,8233,12024,18716,33695,25,6418,176,18716,18716,30218,6418,6418,33695");
		
		 try {
		     // Encode a String into bytes
		     byte[] input = text.getBytes("UTF-8");

		     // Compress the bytes
		     byte[] output = new byte[8912];
		     Deflater compresser = new Deflater();
		     compresser.setInput(input);
		     compresser.finish();
		     int compressedDataLength = compresser.deflate(output);
		     compresser.end();
		     System.out.println("compressed: " + compressedDataLength + " orig: " + text.length());

		     // Decompress the bytes
		     Inflater decompresser = new Inflater();
		     decompresser.setInput(output, 0, compressedDataLength);
		     byte[] result = new byte[100];
		     int resultLength = decompresser.inflate(result);
		     decompresser.end();

		     // Decode the bytes into a String
		     String outputString = new String(result, 0, resultLength, "UTF-8");
		     System.out.println(outputString.length());
		     System.out.println(outputString);
		 } catch(java.io.UnsupportedEncodingException ex) {
		     // handle
		 } catch (java.util.zip.DataFormatException ex) {
		     // handle
		 }
	}
}

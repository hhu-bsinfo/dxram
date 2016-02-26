package de.hhu.bsinfo.dxgraph.conv;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import de.hhu.bsinfo.utils.main.Main;

/**
 * Single threaded converter, expecting edge list in binary form:
 * 8 bytes source nodeId and 8 bytes destination node id.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 *
 */
public class BinaryEdgeListToOel extends Converter 
{
	/**
	 * Main entry point.
	 * @param args Console arguments.
	 */
	public static void main(final String[] args) {
		Main main = new BinaryEdgeListToOel();
		main.run(args);
	}
	
	protected BinaryEdgeListToOel() {
		super("Convert a binary edge list to an ordered edge list (text file)");
	}

	@Override
	protected int parse(String p_inputPath) {
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(p_inputPath, "r");
		} catch (FileNotFoundException e) {
			System.out.println("Opening input file " + p_inputPath + " failed: " + e.getMessage());
			return -1;
		}
		
		ByteBuffer buffer = ByteBuffer.allocate(1024 * 8 * 2);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		
		try {
			long fileLength = file.length();
			long bytesRead = 0;
			do
			{			
				int read = file.read(buffer.array());
				if (read == -1)
					break;
				
				bytesRead += read;
			
				while (buffer.hasRemaining())
				{
					Long srcNode = buffer.getLong();
					Long destNode = buffer.getLong();
					
					processEdge(srcNode, destNode);
					updateProgress("BinaryDataReading", bytesRead, fileLength);
				}
				
				buffer.clear();
				
				
			}
			while (bytesRead < fileLength);
			
			file.close();
		} catch (IOException e) {
			System.out.println("Reading from input file failed: " + e.getMessage());
			return -2;
		}
		
		return 0;
	}

}

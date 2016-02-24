package de.hhu.bsinfo.dxgraph.conv;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.hhu.bsinfo.utils.main.Main;

public class TextEdgeListToOel extends Converter 
{
	/**
	 * Main entry point.
	 * @param args Console arguments.
	 */
	public static void main(final String[] args) {
		Main main = new TextEdgeListToOel();
		main.run(args);
	}
	
	protected TextEdgeListToOel() {
		super("Convert a text edge list to an ordered edge list (text file)");
	}

	@Override
	protected int parse(String p_inputPath) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(p_inputPath));
		} catch (FileNotFoundException e1) {
			System.out.println("Opening buffered reader failed: " + e1.getMessage());
			return -1;
		}
		long fileSize = 0;
		try {
			RandomAccessFile raf = new RandomAccessFile(p_inputPath, "r");
			fileSize = raf.length();
			raf.close();
		} catch (IOException e2) {
		}
		
		System.out.println("Caching input of edge list " + p_inputPath);

		long lineCount = 0;
		long readByteCount = 0;
		while (true)
		{			
			String line = null;
			try {
				line = reader.readLine();
			} catch (IOException e) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
				System.out.println("Reading line failed: " + e.getMessage());
				return -2;
			}
			
			lineCount++;
			if (line == null) // eof
				break;
			
			readByteCount += line.length() + 1;
			
			String[] tokens = line.split(" ");
			if (tokens.length != 2)
			{
				System.out.println("Invalid token count " + tokens.length + " in line " + lineCount + ", skipping");
				continue;
			}
			
			Long srcNode = Long.parseLong(tokens[0]);
			Long destNode = Long.parseLong(tokens[1]);
			
			processEdge(srcNode, destNode);
			updateProgress("ByteDataPos", readByteCount, fileSize);
		}
		
		try {
			reader.close();
		} catch (IOException e) {
		}
		
		return 0;
	}

}

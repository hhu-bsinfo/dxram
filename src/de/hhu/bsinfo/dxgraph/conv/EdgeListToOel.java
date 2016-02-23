package de.hhu.bsinfo.dxgraph.conv;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.Main;

public class EdgeListToOel extends Main 
{
	private static final Argument ARG_INPUT = new Argument("in", null, false, "Edge list input file");
	private static final Argument ARG_OUTPUT = new Argument("out", "./", true, "Ordered edge list output file");
	private static final Argument ARG_FILE_COUNT = new Argument("outFileCount", 1, true, "Split data into multiple files (each approx. same size)");;

	/**
	 * Main entry point.
	 * @param args Console arguments.
	 */
	public static void main(final String[] args) {
		Main main = new EdgeListToOel();
		main.run(args);
	}
	
	protected EdgeListToOel() {
		super("Convert a binary edge list to an ordered edge list (text file)");
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_INPUT);
		p_arguments.setArgument(ARG_OUTPUT);
		p_arguments.setArgument(ARG_FILE_COUNT);
	}

	@Override
	protected int main(ArgumentList p_arguments) 
	{
		String inputPath = p_arguments.getArgumentValue(ARG_INPUT);
		String outputPath = p_arguments.getArgumentValue(ARG_OUTPUT);
		int fileCount = p_arguments.getArgumentValue(ARG_FILE_COUNT);
		
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(inputPath, "r");
		} catch (FileNotFoundException e) {
			System.out.println("Opening input file " + inputPath + " failed: " + e.getMessage());
			return -1;
		}
		
		System.out.println("Caching input of edge list " + inputPath);
		
		VertexStorage storage = new VertexStorageHashMap();
		
		ByteBuffer buffer = ByteBuffer.allocate(1024 * 8 * 2);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		
		try {
			while (file.getFilePointer() != file.length())
			{			
				file.read(buffer.array());
			
				while (buffer.hasRemaining())
				{
					Vertex vertex = null;
					Long srcNode = buffer.getLong();
					Long destNode = buffer.getLong();
					
					// adjust zero based labels
					srcNode++;
					destNode++;
					
					vertex = storage.get(srcNode);
					if (vertex == null)
					{
						vertex = new Vertex(srcNode);
					}
					
					vertex.getNeighbours().add(destNode);
					storage.put(vertex);
					
					vertex = storage.get(destNode);
					if (vertex == null)
					{
						vertex = new Vertex(destNode);
					}
					
					vertex.getNeighbours().add(srcNode);
					storage.put(vertex);
				}
				
				buffer.clear();
			}
			
			file.close();
		} catch (IOException e) {
			System.out.println("Caching from input file failed: " + e.getMessage());
			return -2;
		}
		
		
		// adjust output path
		if (!outputPath.endsWith("/"))
			outputPath += "/";
		
		// also equals vertex count
		long highestId = storage.getHighestID();
		long vertexCount = highestId;
		long rangeStart = 0;
		long rangeEnd = 0;
		long processed = 1; // start with 1 based IDs
		
		System.out.println("Dumping " + vertexCount + " vertices to " + fileCount + " files...");
		
		for (int i = 0; i < fileCount; i++)
		{
			rangeStart = processed;
			rangeEnd = rangeStart + (vertexCount / fileCount);
			if (rangeEnd >= highestId)
				rangeEnd = highestId + 1; // don't forget last id, would be excluded otherwise
			
			try {
				file = new RandomAccessFile(outputPath + "out.oel." + i, "rw");
				if (!storage.dumpOrdered(file, rangeStart, rangeEnd))
				{
					System.out.println("Dumping from vertex storage [" + rangeStart + ", " + rangeEnd + "] failed.");
					continue;
				}
				
				file.close();
			} catch (IOException e) {
				System.out.println("Dumping to out file failed: " + e.getMessage());
				continue;
			}
			
			processed += rangeEnd - rangeStart;
			
			System.out.println("Dumping [" + rangeStart + ", " + rangeEnd + "] to file done");
		}
		
		System.out.println("Done converting, output in " + outputPath);
		
		return 0;
	}

}

package de.hhu.bsinfo.dxgraph.conv;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

import de.hhu.bsinfo.dxcompute.run.DXComputePipeline;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.Main;

public class EdgeListToOel extends Main 
{
	private static final Argument ARG_INPUT = new Argument("in", "edge_list", false, "Edge list input file");
	private static final Argument ARG_OUTPUT = new Argument("out", "out.oel", true, "Ordered edge list output file");
	private static final Argument ARG_MAX_OUT_FILE_SIZE = new Argument("maxOutFileSize", 1024 * 1024 * 1024L, true, "Max output file size in bytes");
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
		p_arguments.setArgument(ARG_MAX_OUT_FILE_SIZE);
		p_arguments.setArgument(ARG_FILE_COUNT);
	}

	@Override
	protected int main(ArgumentList p_arguments) 
	{
		String inputPath = p_arguments.getArgumentValue(ARG_INPUT);
		String outputPath = p_arguments.getArgumentValue(ARG_OUTPUT);
		long maxOutFileSize = p_arguments.getArgumentValue(ARG_MAX_OUT_FILE_SIZE);
		int fileCount = p_arguments.getArgumentValue(ARG_FILE_COUNT);
		
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(inputPath, "r");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		VertexStorage storage = new VertexStorageHashMap();
		ArrayList<Vertex> graph = new ArrayList<Vertex>(); 
		
		ByteBuffer buffer = ByteBuffer.allocate(1024 * 8 * 2);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		
		try {
			while (file.getFilePointer() != file.length())
			{			
				int read = 0;
				try {
					read = file.read(buffer.array());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			storage.dumpOrdered(new RandomAccessFile("out.oel", "rw"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return 0;
	}

}

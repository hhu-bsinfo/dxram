package de.hhu.bsinfo.dxgraph.conv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.Main;

public abstract class Converter extends Main 
{
	private static final Argument ARG_INPUT = new Argument("in", null, false, "Input file of specific format");
	private static final Argument ARG_OUTPUT = new Argument("out", "./", true, "Ordered edge list output file location");
	private static final Argument ARG_FILE_COUNT = new Argument("outFileCount", 1, true, "Split data into multiple files (each approx. same size)");
	private static final Argument ARG_INPUT_DIRECTED_EDGES = new Argument("inputDirectedEdges", true, true, "Specify if the input file contains directed or undirected edges");
	
	private VertexStorage m_storage = null;
	private boolean m_isDirected = false;
	
	private float m_prevProgress = 0;
	
	protected Converter(final String p_description) {
		super(p_description);
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_INPUT);
		p_arguments.setArgument(ARG_OUTPUT);
		p_arguments.setArgument(ARG_FILE_COUNT);
		p_arguments.setArgument(ARG_INPUT_DIRECTED_EDGES);
	}
	
	protected abstract int parse(final String p_inputPath);
	
	protected void processEdge(final long p_hashValueSrcVertex, final long p_hashValueDestVertex)
	{
		long srcVertexId = m_storage.getVertexId(p_hashValueSrcVertex);
		long destVertexId = m_storage.getVertexId(p_hashValueDestVertex);
		
		m_storage.putNeighbour(srcVertexId, destVertexId);
		if (m_isDirected) {
			m_storage.putNeighbour(destVertexId, srcVertexId);
		}
	}
	
	protected void updateProgress(final String p_msg, final long p_curCount, final long p_totalCount)
	{
		float curProgress = ((float) p_curCount) / p_totalCount;
		if (curProgress - m_prevProgress > 0.01)
		{
			if (curProgress > 1.0f) {
				curProgress = 1.0f;
			}
			m_prevProgress = curProgress;
			System.out.println("Progress(" + p_msg + "): " + (int)(curProgress * 100) + "%\r");
		}
	}

	@Override
	protected int main(ArgumentList p_arguments) 
	{
		String inputPath = p_arguments.getArgumentValue(ARG_INPUT);
		String outputPath = p_arguments.getArgumentValue(ARG_OUTPUT);
		int fileCount = p_arguments.getArgumentValue(ARG_FILE_COUNT);
		m_isDirected = p_arguments.getArgumentValue(ARG_INPUT_DIRECTED_EDGES);
		
		m_storage = new VertexStorageSimple();
		
		System.out.println("Parsing input " + inputPath + "...");
		
		int ret = parse(inputPath);
		if (ret != 0)
		{
			System.out.println("Parsing " + inputPath + " failed: " + ret);
			return ret;
		}
		
		resetProgress();
		
		System.out.println("Parsing done, " + m_storage.getTotalVertexCount() + " vertices and " + m_storage.getTotalEdgeCount() + " edges");
		
		dumpToFiles(outputPath, fileCount);
		
		System.out.println("Done converting, output in " + outputPath);
		
		return 0;
	}
	
	private void resetProgress()
	{
		m_prevProgress = 0;	
	}
	
	private void dumpToFiles(final String p_outputPath, final int p_fileCount)
	{
		// adjust output path
		String outputPath = p_outputPath;
		
		if (!outputPath.endsWith("/"))
			outputPath += "/";
		
		// also equals vertex count
		long vertexCount = m_storage.getTotalVertexCount();
		long rangeStart = 0;
		long rangeEnd = 0;
		long processed = 0;
		
		System.out.println("Dumping " + vertexCount + " vertices to " + p_fileCount + " files...");
		
		for (int i = 0; i < p_fileCount; i++)
		{
			rangeStart = processed;
			rangeEnd = rangeStart + (vertexCount / p_fileCount);
			if (rangeEnd >= vertexCount)
				rangeEnd = vertexCount;
			
			try {
				File file = new File(outputPath + "out.oel." + i);
				if (file.exists())
					file.delete();
				
				File fileInfo = new File(outputPath + "out.ioel." + i);
				if (fileInfo.exists())
					fileInfo.delete();
				
				BufferedWriter raf = new BufferedWriter(new FileWriter(file));
				BufferedWriter raf2 = new BufferedWriter(new FileWriter(fileInfo));
				if (!dumpOrdered(raf, raf2, rangeStart, rangeEnd))
				{
					System.out.println("Dumping from vertex storage [" + rangeStart + ", " + rangeEnd + "] failed.");
					raf.close();
					raf2.close();
					continue;
				}
				
				raf.close();
				raf2.close();
			} catch (IOException e) {
				System.out.println("Dumping to out file failed: " + e.getMessage());
				continue;
			}
			
			processed += rangeEnd - rangeStart;
			
			System.out.println("Dumping [" + rangeStart + ", " + rangeEnd + "] to file done");
		}
	}
	
	private boolean dumpOrdered(final BufferedWriter p_file, final BufferedWriter p_infoFile, final long p_rangeStartIncl, final long p_rangeEndExcl)
	{
		// write header (count)
		try {
			p_file.write(Long.toString(p_rangeEndExcl - p_rangeStartIncl) + "\n");
			p_infoFile.write("Vertices: " + Long.toString(p_rangeEndExcl - p_rangeStartIncl) + "\n");
		} catch (IOException e) {
			return false;
		}
		
		long edgeCount = 0;
		long vertexCount = 0;
		for (long i = p_rangeStartIncl; i < p_rangeEndExcl; i++)
		{
			Vertex v = m_storage.getVertex(i);
			if (v != null)
			{
				String str = new String();
				
				//str += v.getID() + ":";
				
				boolean first = true;
				List<Long> neighbours = v.getNeighbours();
				for (long neighbourId : neighbours)
				{
					if (first)
						first = false;
					else
						str += ",";
					
					str += neighbourId;
					edgeCount++;
				}
				
				str += "\n";
				
				try {
					p_file.write(str);
				} catch (IOException e) {
					return false;
				}
				
				vertexCount++;
				updateProgress("TotalVerticesToFiles", vertexCount, p_rangeEndExcl - p_rangeStartIncl);
			} 
			else
			{
				try {
					p_file.write("\n");
				} catch (IOException e) {
					return false;
				}
			}				
		}
		
		try {
			p_infoFile.write("Edges: " + Long.toString(edgeCount) + "\n");
			p_file.flush();
			p_infoFile.flush();
		} catch (IOException e) {
		}
		
		return true;		
	}
}

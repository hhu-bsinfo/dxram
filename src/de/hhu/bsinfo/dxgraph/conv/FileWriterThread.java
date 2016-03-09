package de.hhu.bsinfo.dxgraph.conv;

public abstract class FileWriterThread extends ConverterThread
{
	protected String m_outputPath = null;
	protected int m_id = -1;
	protected long m_idRangeStartIncl = -1;
	protected long m_idRangeEndExcl = -1;
	protected VertexStorage m_storage = null;
	
	public FileWriterThread(final String p_outputPath, final int p_id, final long p_idRangeStartIncl, final long p_idRangeEndExcl, final VertexStorage p_storage)
	{
		super("BinaryFileWriter " + p_id);
		
		m_outputPath = p_outputPath;
		m_id = p_id;
		m_idRangeStartIncl = p_idRangeStartIncl;
		m_idRangeEndExcl = p_idRangeEndExcl;
		m_storage = p_storage;
	}
	
	public abstract void run();
}

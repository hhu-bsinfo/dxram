package de.hhu.bsinfo.dxgraph.conv;

import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.hhu.bsinfo.utils.Pair;

public abstract class FileReaderThread extends ConverterThread
{
	protected String m_inputPath = null;
	protected Queue<Pair<Long, Long>> m_bufferQueue = null;
	protected int m_maxQueueSize = 100000;
	
	public FileReaderThread(String p_inputPath, final Queue<Pair<Long, Long>> p_bufferQueue, final int p_maxQueueSize) {
		super("FileReader " + p_inputPath);
		
		m_inputPath = p_inputPath;
		m_bufferQueue = p_bufferQueue;
		m_maxQueueSize = p_maxQueueSize;
	}
	
	@Override
	public void run () {
		m_errorCode = parse();
	}
	
	public abstract int parse();
}

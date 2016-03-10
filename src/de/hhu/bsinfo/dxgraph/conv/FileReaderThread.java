package de.hhu.bsinfo.dxgraph.conv;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.hhu.bsinfo.utils.Pair;

public abstract class FileReaderThread extends ConverterThread
{
	protected String m_inputPath = null;
	protected ConcurrentLinkedQueue<Pair<Long, Long>> m_bufferQueue = null;
	protected int m_maxQueueSize = 100000;
	
	private Timer m_outputThread;
	
	public FileReaderThread(String p_inputPath, final ConcurrentLinkedQueue<Pair<Long, Long>> p_bufferQueue, final int p_maxQueueSize) {
		super("FileReader " + p_inputPath);
		
		m_inputPath = p_inputPath;
		m_bufferQueue = p_bufferQueue;
		m_maxQueueSize = p_maxQueueSize;
		
		m_outputThread = new Timer(p_inputPath);
	}
	
	@Override
	public void run () {
		m_outputThread.scheduleAtFixedRate(new OutputThread(), 1000, 1000);
		m_errorCode = parse();
		m_outputThread.cancel();
	}
	
	public abstract int parse();
	
	private class OutputThread extends TimerTask
	{
		@Override
		public void run() {
			System.out.println("BufferQueue: " + ((int) ((((float) m_bufferQueue.size()) / m_maxQueueSize) * 100)) + "%");
			System.out.flush();
		}
	}
}

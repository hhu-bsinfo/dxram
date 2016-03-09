package de.hhu.bsinfo.dxgraph.conv;

import java.util.concurrent.ConcurrentLinkedQueue;

import de.hhu.bsinfo.utils.Pair;

public abstract class FileReaderThread extends ConverterThread
{
	protected String m_inputPath = null;
	protected ConcurrentLinkedQueue<Pair<Long, Long>> m_bufferQueue = null;
	protected int m_maxQueueSize = 100000;
	
	private OutputThread m_outputThread;
	
	public FileReaderThread(String p_inputPath, final ConcurrentLinkedQueue<Pair<Long, Long>> p_bufferQueue, final int p_maxQueueSize) {
		super("FileReader " + p_inputPath);
		
		m_inputPath = p_inputPath;
		m_bufferQueue = p_bufferQueue;
		m_maxQueueSize = p_maxQueueSize;
		
		m_outputThread = new OutputThread(p_inputPath);
	}
	
	@Override
	public void run () {
		m_outputThread.start();
		m_errorCode = parse();
		m_outputThread.setRun(false);
		try {
			m_outputThread.join();
		} catch (InterruptedException e) {
		}
	}
	
	public abstract int parse();
	
	private class OutputThread extends Thread
	{
		private volatile boolean m_run = true;
		
		public OutputThread(final String p_name)
		{
			super("OutputThread " + p_name);
		}
		
		public void setRun(final boolean p_run) {
			m_run = p_run;
		}
		
		@Override
		public void run() {
			while (m_run)
			{
				System.out.println("BufferQueue: " + ((int) ((((float) m_bufferQueue.size()) / m_maxQueueSize) * 100)) + "%");
				System.out.flush();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

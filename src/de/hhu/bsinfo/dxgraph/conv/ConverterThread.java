package de.hhu.bsinfo.dxgraph.conv;

public class ConverterThread extends Thread {
	private long m_prevTime = 0;
	protected int m_errorCode = 0;
	
	public ConverterThread(final String p_name) {
		super(p_name);
	}
	
	public int getErrorCode() {
		return m_errorCode;
	}
	
	protected void updateProgress(final String p_msg, final long p_curCount, final long p_totalCount)
	{
		float curProgress = ((float) p_curCount) / p_totalCount;
		long curTime = System.currentTimeMillis();
		if (curTime - m_prevTime > 1000)
		{
			m_prevTime = curTime;
			if (curProgress > 1.0f) {
				curProgress = 1.0f;
			}
	
			System.out.println("Progress(" + p_msg + "): " + curProgress * 100 + "%\r");
		}
	}
}

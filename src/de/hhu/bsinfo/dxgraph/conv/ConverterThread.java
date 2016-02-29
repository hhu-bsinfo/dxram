package de.hhu.bsinfo.dxgraph.conv;

public class ConverterThread extends Thread {
	private float m_prevProgress = 0;
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
		if (curProgress - m_prevProgress > 0.01)
		{
			if (curProgress > 1.0f) {
				curProgress = 1.0f;
			}
			m_prevProgress = curProgress;
			System.out.println("Progress(" + p_msg + "): " + (int)(curProgress * 100) + "%\r");
		}
	}
}

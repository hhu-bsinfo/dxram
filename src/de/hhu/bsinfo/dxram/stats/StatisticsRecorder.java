package de.hhu.bsinfo.dxram.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class StatisticsRecorder 
{
	public static final int INVALID_ID = -1;
	
	private int m_categoryId = INVALID_ID;
	private String m_categoryName = null;
	private boolean m_enabled = true;
	
	private ArrayList<Operation> m_operations = new ArrayList<Operation>();
	
	public StatisticsRecorder(final int p_categoryId, final String p_categoryName)
	{
		m_categoryId = p_categoryId;
		m_categoryName = p_categoryName;
	}
	
	public int getId() {
		return m_categoryId;
	}
	
	public boolean isEnabled() {
		return m_enabled;
	}
	
	public void setEnabled(final boolean p_enabled) {
		m_enabled = p_enabled;
	}
	
	public int createOperation(final String p_name)
	{
		Operation op = new Operation(m_categoryId, m_categoryName, m_operations.size(), p_name);
		
		m_operations.add(op);
		
		return op.getId();
	}
	
	public Operation getOperation(final int p_id)
	{
		Operation op = m_operations.get(p_id);
		op.m_enabled = m_enabled;
		return op;
	}
	
	@Override
	public String toString()
	{
		String str = "[" + m_categoryId + " " +  m_categoryName + ", enabled " + m_enabled + "]";
		for (Operation op : m_operations) {
			str += "\n\t" + op;
		}
		return str;
	}
	
	public static class Operation 
	{
		public static final int INVALID_ID = -1;
		
		private int m_categoryId = INVALID_ID;
		private int m_id = INVALID_ID;
		
		private String m_categoryName = null;
		private String m_name = null;
		
		private boolean m_enabled = true;
		
		// stats per thread, avoids having locks
		private Map<Long, Stats> m_statsMap = new HashMap<Long, Stats>();
		
		private Operation(final int p_categoryId, final String p_categoryName, final int p_id, final String p_name)
		{
			m_categoryId = p_categoryId;
			m_id = p_id;
			m_categoryName = p_categoryName;
			m_name = p_name;
		}
		
		public int getCategoryId() {
			return m_categoryId;
		}
		
		public int getId() {
			return m_id;
		}
		
		public void enter() {
			if (!m_enabled)
				return;
				
			long threadId = Thread.currentThread().getId();
			Stats stats = m_statsMap.get(threadId);
			if (stats == null) {
				stats = new Stats(threadId);
				m_statsMap.put(threadId, stats);
			}
			
			stats.m_opCount++;
			stats.m_timeNsStart = System.nanoTime();
		}
		
		public void enter(final long p_val) {
			if (!m_enabled)
				return;
			
			long threadId = Thread.currentThread().getId();
			Stats stats = m_statsMap.get(threadId);
			if (stats == null) {
				stats = new Stats(threadId);
				m_statsMap.put(threadId, stats);
			}
			
			stats.m_opCount++;
			stats.m_timeNsStart = System.nanoTime();
			stats.m_counter += p_val;
		}
		
		public void enter(final double p_val) {
			if (!m_enabled)
				return;
			
			long threadId = Thread.currentThread().getId();
			Stats stats = m_statsMap.get(threadId);
			if (stats == null) {
				stats = new Stats(threadId);
				m_statsMap.put(threadId, stats);
			}
			
			stats.m_opCount++;
			stats.m_timeNsStart = System.nanoTime();
			stats.m_counter2 += p_val;
		}
		
		public void leave() {
			if (!m_enabled)
				return;
			
			long threadId = Thread.currentThread().getId();
			Stats stats = m_statsMap.get(threadId);
			long duration = System.nanoTime() - stats.m_timeNsStart;
			stats.m_totalTimeNs += duration;
			if (duration < stats.m_shortestTimeNs) {
				stats.m_shortestTimeNs = duration;
			}
			if (duration > stats.m_longestTimeNs) {
				stats.m_longestTimeNs = duration;
			}
		}
		
		@Override
		public String toString() 
		{
			String str = "[" + m_categoryId + " " +  m_categoryName + "] " + m_id + " " + m_name + " (enabled " + m_enabled + "): ";
			for (Entry<Long, Stats> entry : m_statsMap.entrySet())
			{
				str += "\n\t\t" + entry;
			}
			
			return str;
		}
		
		public static class Stats
		{
			private long m_threadId = -1;
			
			private long m_opCount = 0;
			private long m_totalTimeNs = 0;
			private long m_shortestTimeNs = Long.MAX_VALUE;
			private long m_longestTimeNs = Long.MIN_VALUE;
			private long m_counter = 0;
			private double m_counter2 = 0;
			
			// temporary stuff
			private long m_timeNsStart = 0;
			
			private Stats(final long p_threadId)
			{
				m_threadId = p_threadId;
			}
			
			public long getOpCount() {
				return m_opCount;
			}
			
			public long getTotalTimeNs() {
				return m_totalTimeNs;
			}
			
			public long getShortestTimeNs() {
				return m_shortestTimeNs;
			}
			
			public long getLongestTimeNs() {
				return m_longestTimeNs;
			}
			
			public long getAvarageTimeNs() {
				return m_totalTimeNs / m_opCount;
			}
			
			public long getCounter() {
				return m_counter;
			}
			
			public double getCounter2() {
				return m_counter2;
			}
			
			public float getOpsPerSecond() {
				
				return (float) ((1000.0 * 1000.0 * 1000.0) / (((double) m_totalTimeNs) / m_opCount));
			}
			
			@Override
			public String toString() {
				return "Stats[Thread-" + m_threadId + "](m_opCount, " + m_opCount + 
														")(m_totalTimeNs, " + m_totalTimeNs +
														")(m_shortestTimeNs, " + m_shortestTimeNs +
														")(m_longestTimeNs, " + m_longestTimeNs +
														")(avgTimeNs, " + getAvarageTimeNs() + 
														")(opsPerSecond, " + getOpsPerSecond() +
														")(m_counter, " + m_counter +
														")(m_counter2, " + m_counter2 + ")";
			}
		}
	}
}

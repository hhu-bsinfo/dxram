package de.hhu.bsinfo.dxram.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Records statistics for a number of operations. Create one recorder
 * for a subset of operations, for example: category memory management,
 * operations alloc, free, ...
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 *
 */
public class StatisticsRecorder 
{
	public static final int INVALID_ID = -1;
	
	private int m_categoryId = INVALID_ID;
	private String m_categoryName = null;
	private boolean m_enabled = true;
	
	private ArrayList<Operation> m_operations = new ArrayList<Operation>();
	
	/**
	 * Constructor
	 * @param p_categoryId Category identifier for this recorder.
	 * @param p_categoryName Readable category name string.
	 */
	public StatisticsRecorder(final int p_categoryId, final String p_categoryName)
	{
		m_categoryId = p_categoryId;
		m_categoryName = p_categoryName;
	}
	
	/**
	 * Get the category id.
	 * @return Category id.
	 */
	public int getId() {
		return m_categoryId;
	}
	
	/**
	 * Get the name of the recorder (category).
	 * @return Name of the recorder.
	 */
	public String getName() {
		return m_categoryName;
	}
	
	/**
	 * Check if the recorder is enabled to record statistics.
	 * @return True if enabled, false otherwise.
	 */
	public boolean isEnabled() {
		return m_enabled;
	}
	
	/**
	 * Enable/Disable the recorder.
	 * @param p_enabled True to enable, false to disable.
	 */
	public void setEnabled(final boolean p_enabled) {
		m_enabled = p_enabled;
	}
	
	/**
	 * Create a new operation to be recorded within this recorder.
	 * @param p_name Name for the operation.
	 * @return Id to identify the newly created operation.
	 */
	public int createOperation(final String p_name)
	{
		Operation op = new Operation(m_categoryId, m_categoryName, m_operations.size(), p_name);
		
		m_operations.add(op);
		
		return op.getId();
	}
	
	/**
	 * Get an operation from the recorder.
	 * @param p_id Id of the operation to get.
	 * @return Operation referenced by id.
	 */
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
	
	/**
	 * A single operation tracks time, counters, averages etc
	 * of one task/method call, for example: memory management
	 * -> alloc operation
	 * Each operation is part of a recorder.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
	 *
	 */
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
		
		/**
		 * Constructor
		 * @param p_categoryId Id of the category this operation belongs to.
		 * @param p_categoryName Name of the category this operation belongs to.
		 * @param p_id Id of the operation.
		 * @param p_name Name of the operation.
		 */
		private Operation(final int p_categoryId, final String p_categoryName, final int p_id, final String p_name)
		{
			m_categoryId = p_categoryId;
			m_id = p_id;
			m_categoryName = p_categoryName;
			m_name = p_name;
		}
		
		/**
		 * Get the category id this operation belongs to.
		 * @return Category id.
		 */
		public int getCategoryId() {
			return m_categoryId;
		}
		
		/**
		 * Get the id of the operation (within the category)
		 * @return Id of the operation.
		 */
		public int getId() {
			return m_id;
		}
		
		/**
		 * Call this when/before you start/enter the call/operation you want
		 * to record.
		 */
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
		
		/**
		 * Call this when/before you start/enter the call/operation you want
		 * to record.
		 * @param p_val Value to added to the long counter.
		 */
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
		
		/**
		 * Call this when/before you start/enter the call/operation you want
		 * to record.
		 * @param p_val Value to added to the double counter.
		 */
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
		
		/**
		 * Call this when/after you ended/left the call/operation.
		 */
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
		
		/**
		 * Internal state for an operation for statistics.
		 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
		 *
		 */
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
			
			/**
			 * Constructor
			 * @param p_threadId Id of the thread this object is used in.
			 */
			private Stats(final long p_threadId)
			{
				m_threadId = p_threadId;
			}
			
			/**
			 * Get the operation count recorded (i.e. how often was enter called).
			 * @return Operation count.
			 */
			public long getOpCount() {
				return m_opCount;
			}
			
			/**
			 * Get the total amount of time we were in the enter/leave section in ns.
			 * @return Total time in ns.
			 */
			public long getTotalTimeNs() {
				return m_totalTimeNs;
			}
			
			/**
			 * Get the shortest time we spent in the enter/leave section in ns.
			 * @return Shortest time in ns.
			 */
			public long getShortestTimeNs() {
				return m_shortestTimeNs;
			}
			
			/**
			 * Get the longest time we spent in the enter/leave section in ns.
			 * @return Longest time in ns.
			 */
			public long getLongestTimeNs() {
				return m_longestTimeNs;
			}
			
			/**
			 * Get the avarage time we spent in the enter/leave section in ns.
			 * @return Avarage time in ns.
			 */
			public long getAvarageTimeNs() {
				return m_totalTimeNs / m_opCount;
			}
			
			/**
			 * Get the long counter. Depending on the operation, this is used for tracking different things.
			 * @return Long counter value.
			 */
			public long getCounter() {
				return m_counter;
			}
			
			/**
			 * Get the double counter. Depending on the operation, this is used for tracking different things.
			 * @return Double counter value.
			 */
			public double getCounter2() {
				return m_counter2;
			}
			
			/**
			 * Calculate the number of operations per second.
			 * @return Number of operations per second.
			 */
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

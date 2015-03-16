
package de.uniduesseldorf.dxram.core.events;

import de.uniduesseldorf.dxram.core.events.LogWriteListener.LogWriteEvent;

/**
 * Methods for reacting to LogWriteEvents
 * @author Kevin Beineke
 *         11.06.2014
 */
public interface LogWriteListener extends EventListener<LogWriteEvent> {

	// Methods
	@Override
	void triggerEvent(LogWriteEvent p_event);

	// Classes
	/**
	 * Event will be triggered, if a log write access returned
	 * @author Kevin Beineke
	 *         11.06.2014
	 */
	public static class LogWriteEvent extends AbstractEvent {

		//Attributes
		private String m_name;
		private Throwable m_exception;
		private short m_nodeID;
		private int m_writtenDataSize;
		private short m_secLogID;
		private int m_corruptedDataSize;

		// Constructors
		/**
		 * Creates an instance of LogWriteEvent
		 * @param p_name
		 *            the name of the event
		 * @param p_exception
		 *            the exception of the event
		 * @param p_nodeID
		 *            the NodeID of the event source
		 * @param p_writtenDataSize
		 *            the number of written bytes
		 * @param p_secLogID
		 *            the secondary log ID
		 * @param p_corruptedDataSize
		 *            the number of corrupted bytes
		 */
		public LogWriteEvent(final String p_name, final Throwable p_exception, final short p_nodeID,
				final int p_writtenDataSize, final short p_secLogID, final int p_corruptedDataSize) {
			super(p_nodeID);
			m_name = p_name;
			m_exception = p_exception;
			m_nodeID = p_nodeID;
			m_writtenDataSize = p_writtenDataSize;
			m_secLogID = p_secLogID;
			m_corruptedDataSize = p_corruptedDataSize;
		}

		// Getters
		/**
		 * Returns the name
		 * @return the name
		 */
		public final String getName() {
			return m_name;
		}

		/**
		 * Returns the exception
		 * @return the exception
		 */
		public final Throwable getException() {
			return m_exception;
		}

		/**
		 * Returns the NodeID
		 * @return the NodeID
		 */
		public final short getNodeID() {
			return m_nodeID;
		}

		/**
		 * Returns the number of written bytes
		 * @return the number of written bytes
		 */
		public final int getWrittenDataSize() {
			return m_writtenDataSize;
		}

		/**
		 * Returns the secondary log ID
		 * @return the secondary log ID
		 */
		public final short getSecLogID() {
			return m_secLogID;
		}

		/**
		 * Returns the number of corrupted bytes
		 * @return the number of corrupted bytes
		 */
		public final int getCorruptedDataSize() {
			return m_corruptedDataSize;
		}

		// Methods
		/**
		 * Callback function to respond to a successful write access to primary log
		 */
		public void onPrimLogWrite() {}

		/**
		 * Callback function to respond to a successful write access to a secondary log
		 */
		public void onSecLogWrite() {}

		/**
		 * Callback function to respond to an exception during writing in buffer
		 */
		public void onWriteBufferExc() {}

		/**
		 * Callback function to respond to an exception during writing in primary log
		 */
		public void onPrimLogExc() {}

		/**
		 * Callback function to respond to an exception during writing in a secondary log
		 */
		public void onSecLogExc() {}

		/**
		 * Callback function to respond to an exception caused by a filled secondary log
		 */
		public void onSecLogFull() {}

		/**
		 * Callback function to respond to an exception during reorganization
		 */
		public void onReorgExc() {}

		/**
		 * Callback function to respond to an exception during restart
		 */
		public void onRestartExc() {}

		@Override
		protected final String infoString() {
			return "";
		}
	}

}

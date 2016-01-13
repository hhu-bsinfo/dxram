
package de.hhu.bsinfo.dxram.events;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.events.IncomingChunkListener.IncomingChunkEvent;
import de.hhu.bsinfo.utils.Contract;

/**
 * Methods for reacting to IncomingChunkEvents
 * @author Florian Klein
 *         01.08.2012
 */
public interface IncomingChunkListener extends EventListener<IncomingChunkEvent> {

	// Methods
	@Override
	void triggerEvent(IncomingChunkEvent p_event);

	// Classes
	/**
	 * Event will be triggered, when a requested Chunk arrives
	 * @author Florian Klein
	 *         01.08.2012
	 */
	class IncomingChunkEvent extends AbstractEvent {

		// Attributes
		private Chunk m_chunk;

		// Constructors
		/**
		 * Creates an instance of IncomingChunkEvent
		 * @param p_source
		 *            the NodeID from which send the Chunk
		 * @param p_chunk
		 *            the Chunk
		 */
		public IncomingChunkEvent(final short p_source, final Chunk p_chunk) {
			super(p_source);

			Contract.checkNotNull(p_chunk, "no chunk given");

			m_chunk = p_chunk;
		}

		// Getters
		/**
		 * Returns the Chunk
		 * @return the Chunk
		 */
		public final Chunk getChunk() {
			return m_chunk;
		}

		@Override
		protected final String infoString() {
			return m_chunk.toString();
		}

	}

}

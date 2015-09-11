
package de.uniduesseldorf.dxram.core.net;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;

/**
 * A simple cache of direct buffers.
 * Based on sun.nio.ch.Util.BufferCache
 * @author Marc Ewert 06.10.14
 */
final class BufferCache {

	// Constants
	public static final int MAX_MEMORY_CACHED = Core.getConfiguration().getIntValue(ConfigurationConstants.NETWORK_MAX_CACHE_SIZE);

	// Attributes
	private static FixBufferCache m_cache = new FixBufferCache();

	// Constructors
	/**
	 * Creates an instance of BufferCache
	 */
	private BufferCache() {}

	// Methods
	/**
	 * Offers a buffer
	 * @param p_buffer
	 *            the buffer
	 * @return true if the operation was successful
	 */
	public static boolean offer(final ByteBuffer p_buffer) {
		synchronized (m_cache) {
			return m_cache.offer(p_buffer);
		}
	}

	/**
	 * Frees a buffer
	 * @param p_size
	 *            the size of the buffer
	 */
	public static void free(final int p_size) {
		// m_cache.free(p_size);
	}

	/**
	 * Returns a temporary buffer of at least the given size
	 * @param p_size
	 *            the size of the buffer
	 * @return the buffer
	 */
	public static ByteBuffer getDirectBuffer(final int p_size) {
		synchronized (m_cache) {
			return m_cache.getDirectBuffer(p_size);
		}
	}

	// Classes
	/**
	 * Cahe for the ByteBuffers
	 * @author klein 26.03.2015
	 */
	private static class FixBufferCache {

		// Constants
		// The number of temp buffers in our pool
		private static final int TEMP_BUF_POOL_SIZE = 1;

		// Attributes
		// the array of buffers
		private ByteBuffer[] m_buffers = new ByteBuffer[TEMP_BUF_POOL_SIZE];

		// the number of buffers in the cache
		private int m_count;

		// the index of the first valid buffer (undefined if count == 0)
		private int m_start;

		// Constructors
		/**
		 * Creates an instance of FixBufferCache
		 */
		public FixBufferCache() {
			m_buffers = new ByteBuffer[TEMP_BUF_POOL_SIZE];
		}

		// Methods
		/**
		 * Gets the next position
		 * @param p_position
		 *            the current position
		 * @return the next position
		 */
		private int next(final int p_position) {
			return (p_position + 1) % TEMP_BUF_POOL_SIZE;
		}

		/**
		 * Removes and returns a buffer from the cache of at least the given
		 * size (or null if no suitable buffer is found).
		 * @param p_size
		 *            the size of the buffer
		 * @return the buffer
		 */
		private ByteBuffer get(final int p_size) {
			ByteBuffer ret = null;
			ByteBuffer bb;

			synchronized (m_buffers) {
				if (m_count > 0) {
					// search for suitable buffer (often the first buffer will do)
					ret = m_buffers[m_start];
					if (ret.capacity() < p_size) {
						ret = null;
						int i = m_start;
						while ((i = next(i)) != m_start) {
							bb = m_buffers[i];
							if (bb == null) {
								break;
							}
							if (bb.capacity() >= p_size) {
								ret = bb;
								break;
							}
						}
						if (ret != null) {
							// move first element to here to avoid re-packing
							m_buffers[i] = m_buffers[m_start];
						}
					}

					if (ret != null) {
						// remove first element
						m_buffers[m_start] = null;
						m_start = next(m_start);
						m_count--;

						// prepare the buffer and return it
						ret.clear();
						ret.limit(p_size);
					}
				}
			}

			return ret;
		}

		/**
		 * Offers a buffer
		 * @param p_buffer
		 *            the buffer
		 * @return true if the operation was successful
		 */
		private boolean offerFirst(final ByteBuffer p_buffer) {
			boolean ret;

			synchronized (m_buffers) {
				if (m_count >= TEMP_BUF_POOL_SIZE) {
					ret = false;
				} else {
					m_start = (m_start + TEMP_BUF_POOL_SIZE - 1) % TEMP_BUF_POOL_SIZE;
					m_buffers[m_start] = p_buffer;
					m_count++;

					ret = true;
				}
			}

			return ret;
		}

		/**
		 * Checks if the cache is empty
		 * @return true if the cache is empty, false otherwise
		 */
		private boolean isEmpty() {
			synchronized (m_buffers) {
				return m_count == 0;
			}
		}

		/**
		 * Removes and returns the first buffer
		 * @return the removed buffer
		 */
		private ByteBuffer removeFirst() {
			synchronized (m_buffers) {
				assert m_count > 0;
				final ByteBuffer ret = m_buffers[m_start];

				m_buffers[m_start] = null;
				m_start = next(m_start);
				m_count--;

				return ret;
			}
		}

		/**
		 * Offers a buffer
		 * @param p_buffer
		 *            the buffer
		 * @return true if the operation was successful
		 */
		public boolean offer(final ByteBuffer p_buffer) {
			return offerFirst(p_buffer);
		}

		/**
		 * Get a direct buffer
		 * @param p_size
		 *            the size of the buffer
		 * @return a direct buffer
		 */
		public ByteBuffer getDirectBuffer(final int p_size) {
			ByteBuffer buf = null;

			try {
				buf = get(p_size);
			} catch (final Exception e) {
				e.printStackTrace();
			}

			if (buf == null) {

				// No suitable buffer in the cache so we need to allocate a new
				// one. To avoid the cache growing then we remove the first
				// buffer from the cache and free it.
				if (!isEmpty()) {
					removeFirst();
				}

				// LOGGER.info("allocating " + size + "bytes");
				buf = ByteBuffer.allocateDirect(p_size);
			}

			return buf;
		}
	}

}

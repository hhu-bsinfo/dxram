
package de.uniduesseldorf.dxram.core.net;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;

/**
 * A simple cache of direct buffers.
 * Based on sun.nio.ch.Util.BufferCache
 * @author Marc Ewert 06.10.14
 */
public class BufferCache {

	public static final int MAX_MEMORY_CACHED;

	private static final Logger LOGGER = Logger.getLogger(BufferCache.class);

	private static final FixBufferCache CACHE = new FixBufferCache();

	static {
		MAX_MEMORY_CACHED = Core.getConfiguration().getIntValue(ConfigurationConstants.NETWORK_MAX_CACHESIZE);
	}

	public static boolean offer(final ByteBuffer buf) {
		synchronized (CACHE) {
			return CACHE.offer(buf);
		}
	}

	public static void free(final int p_size) {
		// m_cache.free(p_size);
	}

	/**
	 * Returns a temporary buffer of at least the given size
	 */
	public static ByteBuffer getDirectBuffer(final int size) {
		synchronized (CACHE) {
			return CACHE.getDirectBuffer(size);
		}
	}

	private static class FixBufferCache {

		// The number of temp buffers in our pool
		private final int m_tempBufferPoolSize = 1;

		// the array of buffers
		private final ByteBuffer[] m_buffers = new ByteBuffer[m_tempBufferPoolSize];

		// the number of buffers in the cache
		private int m_count;

		// the number of currently used buffers
		// private int used;

		// the index of the first valid buffer (undefined if count == 0)
		private int m_start;

		private int next(final int i) {
			return (i + 1) % m_tempBufferPoolSize;
		}

		/**
		 * Removes and returns a buffer from the cache of at least the given
		 * size (or null if no suitable buffer is found).
		 */
		private ByteBuffer get(final int size) {
			ByteBuffer buf;
			ByteBuffer bb;

			synchronized (m_buffers) {
				if (m_count == 0) {
					// cache is empty
					return null;
				}

				// search for suitable buffer (often the first buffer will do)
				buf = m_buffers[m_start];
				if (buf.capacity() < size) {
					buf = null;
					int i = m_start;
					while ((i = next(i)) != m_start) {
						bb = m_buffers[i];
						if (bb == null) {
							break;
						}
						if (bb.capacity() >= size) {
							buf = bb;
							break;
						}
					}
					if (buf == null) {
						return null;
					}
					// move first element to here to avoid re-packing
					m_buffers[i] = m_buffers[m_start];
				}

				// remove first element
				m_buffers[m_start] = null;
				m_start = next(m_start);
				m_count--;
			}

			// prepare the buffer and return it
			buf.clear();
			buf.limit(size);
			return buf;
		}

		private boolean offerFirst(final ByteBuffer buf) {
			synchronized (m_buffers) {
				if (m_count >= m_tempBufferPoolSize) {
					return false;
				} else {
					m_start = (m_start + m_tempBufferPoolSize - 1) % m_tempBufferPoolSize;
					m_buffers[m_start] = buf;
					m_count++;
					return true;
				}
			}
		}

		private boolean isEmpty() {
			synchronized (m_buffers) {
				return m_count == 0;
			}
		}

		private ByteBuffer removeFirst() {
			ByteBuffer buf;

			synchronized (m_buffers) {
				assert m_count > 0;
				buf = m_buffers[m_start];
				m_buffers[m_start] = null;
				m_start = next(m_start);
				m_count--;
				return buf;
			}
		}

		public boolean offer(final ByteBuffer buf) {
			return offerFirst(buf);
		}

		public ByteBuffer getDirectBuffer(final int size) {
			ByteBuffer buf = null;

			try {
				buf = get(size);
			} catch (final Exception e) {
				e.printStackTrace();
			}

			if (buf == null) {

				// No suitable buffer in the cache so we need to allocate a new
				// one. To avoid the cache growing then we remove the first
				// buffer from the cache and free it.
				if (!isEmpty()) {
					removeFirst();
					LOGGER.info("New max buffer size: " + size + " bytes");
				}

				// LOGGER.info("allocating " + size + "bytes");
				buf = ByteBuffer.allocateDirect(size);
			}

			return buf;
		}
	}

}

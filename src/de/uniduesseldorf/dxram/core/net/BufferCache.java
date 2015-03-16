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
class BufferCache {

	public static final int MAX_MEMORY_CACHED;

	private static final Logger LOGGER = Logger.getLogger(BufferCache.class);

	private static final FixBufferCache m_cache = new FixBufferCache();

	static {
		MAX_MEMORY_CACHED = Core.getConfiguration().getIntValue(ConfigurationConstants.NETWORK_MAX_CACHESIZE);
	}

	public static boolean offer(ByteBuffer buf) {
		synchronized (m_cache) {
			return m_cache.offer(buf);
		}
	}

	public static void free(int p_size) {
		// m_cache.free(p_size);
	}

	/**
	 * Returns a temporary buffer of at least the given size
	 */
	public static ByteBuffer getDirectBuffer(int size) {
		synchronized (m_cache) {
			return m_cache.getDirectBuffer(size);
		}
	}

	private static class FixBufferCache {

		// The number of temp buffers in our pool
		private final int TEMP_BUF_POOL_SIZE = 1;

		// the array of buffers
		private final ByteBuffer[] buffers = new ByteBuffer[TEMP_BUF_POOL_SIZE];

		// the number of buffers in the cache
		private int count;

		// the number of currently used buffers
		// private int used;

		// the index of the first valid buffer (undefined if count == 0)
		private int start;

		private int next(int i) {
			return (i + 1) % TEMP_BUF_POOL_SIZE;
		}

		/**
		 * Removes and returns a buffer from the cache of at least the given
		 * size (or null if no suitable buffer is found).
		 */
		private ByteBuffer get(int size) {
			ByteBuffer buf;

			synchronized (buffers) {
				if (count == 0)
					return null; // cache is empty

				// search for suitable buffer (often the first buffer will do)
				buf = buffers[start];
				if (buf.capacity() < size) {
					buf = null;
					int i = start;
					while ((i = next(i)) != start) {
						ByteBuffer bb = buffers[i];
						if (bb == null)
							break;
						if (bb.capacity() >= size) {
							buf = bb;
							break;
						}
					}
					if (buf == null)
						return null;
					// move first element to here to avoid re-packing
					buffers[i] = buffers[start];
				}

				// remove first element
				buffers[start] = null;
				start = next(start);
				count--;
			}

			// prepare the buffer and return it
			buf.clear();
			buf.limit(size);
			return buf;
		}

		private boolean offerFirst(ByteBuffer buf) {
			synchronized (buffers) {
				if (count >= TEMP_BUF_POOL_SIZE) {
					return false;
				} else {
					start = (start + TEMP_BUF_POOL_SIZE - 1) % TEMP_BUF_POOL_SIZE;
					buffers[start] = buf;
					count++;
					return true;
				}
			}
		}

		private boolean isEmpty() {
			synchronized (buffers) {
				return count == 0;
			}
		}

		private ByteBuffer removeFirst() {
			synchronized (buffers) {
				assert count > 0;
				ByteBuffer buf = buffers[start];
				buffers[start] = null;
				start = next(start);
				count--;
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
			} catch (Exception e) {
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

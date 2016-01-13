
package de.hhu.bsinfo.utils.unsafe;

import java.util.Iterator;

import sun.misc.Unsafe;

/**
 * An UnsafeArray for long
 * @author Florian Klein
 *         04.07.2014
 */
public final class LongArray implements Iterable<Long> {

	// Constants
	private static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();

	// Attributes
	private final UnsafeArray m_array;

	// Constructors
	/**
	 * Creates an instance of IntegerArray
	 * @param p_count
	 *            the element count
	 */
	public LongArray(final int p_count) {
		m_array = new UnsafeArray(8, p_count);
	}

	// Methods
	/**
	 * Get the size of the array
	 * @return the size
	 */
	public int getSize() {
		return m_array.getSize();
	}

	/**
	 * Get the elment at the given index
	 * @param p_index
	 *            the index
	 * @return the element
	 */
	public long get(final int p_index) {
		long ret;
		long address;

		address = m_array.get(p_index);
		ret = UNSAFE.getLong(address);

		return ret;
	}

	/**
	 * Set the element at the given index
	 * @param p_index
	 *            the index
	 * @param p_value
	 *            the value
	 */
	public void set(final int p_index, final long p_value) {
		long address;

		address = m_array.get(p_index);
		UNSAFE.putLong(address, p_value);
	}

	@Override
	public Iterator<Long> iterator() {
		return new LongIterator();
	}

	// Classes
	/**
	 * Iterator for an UnsafeArray with longs
	 * @author Florian Klein
	 *         04.07.2014
	 */
	private final class LongIterator implements Iterator<Long> {

		// Attributes
		private int m_index;

		// Constructors
		/**
		 * Creates an instance of IntegerIterator
		 */
		private LongIterator() {
			m_index = 0;
		}

		// Methods
		@Override
		public boolean hasNext() {
			return m_index < getSize() - 1;
		}

		@Override
		public Long next() {
			return get(m_index++);
		}

		@Override
		public void remove() {}

	}

}

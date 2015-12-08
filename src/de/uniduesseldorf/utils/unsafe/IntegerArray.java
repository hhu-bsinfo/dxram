
package de.uniduesseldorf.utils.unsafe;

import java.util.Iterator;

import sun.misc.Unsafe;

/**
 * An UnsafeList for integers
 * @author Florian Klein
 *         04.07.2014
 */
public final class IntegerArray implements Iterable<Integer> {

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
	public IntegerArray(final int p_count) {
		m_array = new UnsafeArray(4, p_count);
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
	public int get(final int p_index) {
		int ret;
		long address;

		address = m_array.get(p_index);
		ret = UNSAFE.getInt(address);

		return ret;
	}

	/**
	 * Set the element at the given index
	 * @param p_index
	 *            the index
	 * @param p_value
	 *            the value
	 */
	public void set(final int p_index, final int p_value) {
		long address;

		address = m_array.get(p_index);
		UNSAFE.putInt(address, p_value);
	}

	@Override
	public Iterator<Integer> iterator() {
		return new IntegerIterator();
	}

	// Classes
	/**
	 * Iterator for an UnsafeArray with integers
	 * @author Florian Klein
	 *         04.07.2014
	 */
	private final class IntegerIterator implements Iterator<Integer> {

		// Attributes
		private int m_index;

		// Constructors
		/**
		 * Creates an instance of IntegerIterator
		 */
		private IntegerIterator() {
			m_index = 0;
		}

		// Methods
		@Override
		public boolean hasNext() {
			return m_index < getSize() - 1;
		}

		@Override
		public Integer next() {
			return get(m_index++);
		}

		@Override
		public void remove() {}

	}

}

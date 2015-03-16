
package de.uniduesseldorf.dxram.utils.unsafe;

import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Array List using the unsafe class to allocate elements
 * @author Florian Klein
 *         04.07.2014
 */
public final class UnsafeArrayList extends AbstractUnsafeList {

	// Constants
	private static final int DEFAULT_INITIAL_SIZE = 32;

	// Attributes
	private UnsafeArray m_array;

	private int m_size;
	private int m_capacity;;

	// Constructors
	/**
	 * Creates an instance of UnsafeArrayList
	 */
	protected UnsafeArrayList() {
		this(DEFAULT_INITIAL_SIZE, 0);
	}

	/**
	 * Creates an instance of UnsafeArrayList
	 * @param p_initialSize
	 *            the initial size of the array
	 */
	protected UnsafeArrayList(final int p_initialSize) {
		this(p_initialSize, 0);
	}

	/**
	 * Creates an instance of UnsafeArrayList
	 * @param p_initialSize
	 *            the initial size of the array
	 * @param p_elementSize
	 *            the size of an element
	 */
	private UnsafeArrayList(final int p_initialSize, final int p_elementSize) {
		super(p_elementSize);

		Contract.check(p_elementSize >= 0, "invalid element size");
		Contract.check(p_initialSize > 0, "invalid initial size");

		m_array = null;

		m_size = 0;
		m_capacity = p_initialSize;
	}

	/**
	 * Gets the current capacity
	 * @return the capacity
	 */
	protected int getCapacity() {
		return m_capacity;
	}

	// Methods
	@Override
	protected int getSize() {
		return m_size;
	}

	@Override
	protected long get(final int p_position) {
		long ret;

		if (p_position < 0 || p_position >= m_size) {
			throw new IndexOutOfBoundsException();
		}

		if (m_array == null) {
			m_array = new UnsafeArray(getElementSize(), m_capacity);
		}

		ret = m_array.get(p_position);

		return ret;
	}

	@Override
	protected long put(final int p_position) {
		long ret;
		int elementSize;

		if (p_position < 0 || p_position > m_size) {
			throw new IndexOutOfBoundsException();
		}

		elementSize = getElementSize();
		if (m_array == null) {
			m_array = new UnsafeArray(elementSize, m_capacity);
		}

		if (m_size == m_array.getSize()) {
			m_capacity <<= 1;
			m_array.resize(m_capacity);
		}

		ret = m_array.get(p_position);
		for (int i = m_size - 1;i >= p_position;i--) {
			UNSAFE.copyMemory(m_array.get(i), m_array.get(i + 1), elementSize);
		}

		m_size++;

		return ret;
	}

	@Override
	protected void remove(final int p_position) {
		int elementSize;

		if (p_position < 0 || p_position >= m_size) {
			throw new IndexOutOfBoundsException();
		}

		elementSize = getElementSize();
		for (int i = p_position;i < m_size - 1;i++) {
			UNSAFE.copyMemory(m_array.get(i + 1), m_array.get(i), elementSize);
		}

		m_size--;

		if (m_size == m_array.getSize() / 2) {
			m_capacity >>= 1;
			m_array.resize(m_capacity);
		}
	}

	@Override
	protected UnsafeArrayList subList(final int p_from, final int p_to) {
		UnsafeArrayList ret;
		int elementSize;

		if (p_from < 0 || p_from >= m_size || p_to < 0 || p_to >= m_size || p_from > p_to) {
			throw new IndexOutOfBoundsException();
		}

		elementSize = getElementSize();
		ret = new UnsafeArrayList(elementSize, p_to - p_from);
		for (int i = 0;i < p_to - p_from;i++) {
			UNSAFE.copyMemory(m_array.get(p_from + i), ret.m_array.get(i), elementSize);
		}

		return ret;
	}

	@Override
	protected void clear() {
		m_size = 0;
	}

}

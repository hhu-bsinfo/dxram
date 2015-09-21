
package de.uniduesseldorf.dxram.utils.unsafe;

import sun.misc.Unsafe;

import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Array using the unsafe class to allocate elements
 * @author Florian Klein
 *         04.07.2014
 */
public final class UnsafeArray {

	// Constants
	private static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();

	// Attributes
	private long m_head;

	private final long m_elementSize;
	private int m_size;

	// Constructors
	/**
	 * Creates an instance of UnsafeArray
	 * @param p_elementSize
	 *            the size of an element
	 * @param p_elementCount
	 *            the number of elements
	 */
	protected UnsafeArray(final int p_elementSize, final int p_elementCount) {
		long size;

		Contract.check(p_elementSize > 0, "invalid element size");
		Contract.check(p_elementCount > 0, "invalid element count");

		m_elementSize = p_elementSize;
		m_size = p_elementCount;

		size = m_elementSize * m_size;
		m_head = UNSAFE.allocateMemory(size);
		UNSAFE.setMemory(m_head, size, (byte) 0);
	}

	/**
	 * Get the array size
	 * @return the size
	 */
	protected int getSize() {
		return m_size;
	}

	// Methods
	/**
	 * Get the elment at the given index
	 * @param p_index
	 *            the index
	 * @return the element
	 */
	protected long get(final int p_index) {
		long ret;

		if (p_index < 0 || p_index >= m_size) {
			throw new IndexOutOfBoundsException();
		}

		ret = m_head + m_elementSize * p_index;

		return ret;
	}

	/**
	 * Resizes the array
	 * @param p_elementCount
	 *            the new element count
	 */
	protected void resize(final int p_elementCount) {
		long address;
		long size;

		size = m_elementSize * p_elementCount;
		address = UNSAFE.allocateMemory(size);
		UNSAFE.setMemory(address, size, (byte) 0);
		UNSAFE.copyMemory(m_head, address, m_elementSize * Math.min(m_size, p_elementCount));

		UNSAFE.freeMemory(m_head);

		m_head = address;
		m_size = p_elementCount;
	}

}

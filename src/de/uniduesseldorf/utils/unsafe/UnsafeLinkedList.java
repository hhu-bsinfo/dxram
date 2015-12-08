
package de.uniduesseldorf.utils.unsafe;

import sun.misc.Unsafe;

/**
 * Doubly-linked List using the unsafe class to allocate elements
 * @author Florian Klein
 *         04.07.2014
 */
public final class UnsafeLinkedList extends AbstractUnsafeList {

	// Constants
	private static final int HEAD_SIZE = 16;
	private static final int PREDECESSOR_OFFSET = 0;
	private static final int SUCCESSOR_OFFSET = 8;
	private static final int DATA_OFFSET = 16;

	private static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();

	// Attributes
	private long m_head;
	private long m_tail;

	private int m_size;

	// Constructors
	/**
	 * Creates an instance of UnsafeList
	 */
	protected UnsafeLinkedList() {
		this(0);
	}

	/**
	 * Creates an instance of UnsafeList
	 * @param p_elementSize
	 *            the size of an element
	 */
	private UnsafeLinkedList(final int p_elementSize) {
		super(p_elementSize);

		m_head = 0;
		m_tail = 0;

		m_size = 0;
	}

	/**
	 * Get the size
	 * @return the size
	 */
	@Override
	protected int getSize() {
		return m_size;
	}

	// Methods
	/**
	 * Get the element at the given position
	 * @param p_position
	 *            the position of the element
	 * @return the element
	 */
	@Override
	protected long get(final int p_position) {
		long ret;
		long address;

		if (p_position < 0 || p_position >= m_size) {
			throw new IndexOutOfBoundsException();
		} else if (p_position == 0) {
			address = m_head;
		} else if (p_position == m_size - 1) {
			address = m_tail;
		} else {
			address = m_head;
			for (int i = 0; i < p_position; i++) {
				address = UNSAFE.getLong(address + SUCCESSOR_OFFSET);
			}
		}

		ret = address + DATA_OFFSET;

		return ret;
	}

	/**
	 * Adds an element to the list at the given position. The element at the given position ist pushed back.
	 * @param p_position
	 *            the position to put the element
	 * @return the address of the element
	 */
	@Override
	protected long put(final int p_position) {
		long ret;
		long address;
		long predecessor;
		long successor;

		if (p_position < 0 || p_position > m_size) {
			throw new IndexOutOfBoundsException();
		}

		address = UNSAFE.allocateMemory(HEAD_SIZE + getElementSize());
		if (p_position == 0) {
			if (m_head != 0) {
				UNSAFE.putLong(m_head + PREDECESSOR_OFFSET, address);
			}

			UNSAFE.putLong(address + PREDECESSOR_OFFSET, 0);
			UNSAFE.putLong(address + SUCCESSOR_OFFSET, m_head);
			m_head = address;

			if (m_tail == 0) {
				m_tail = address;
			}
		} else if (p_position == m_size) {
			UNSAFE.putLong(m_tail + SUCCESSOR_OFFSET, address);

			UNSAFE.putLong(address + PREDECESSOR_OFFSET, m_tail);
			UNSAFE.putLong(address + SUCCESSOR_OFFSET, 0);
			m_tail = address;
		} else {
			predecessor = m_head;
			for (int i = 1; i < p_position; i++) {
				predecessor = UNSAFE.getLong(predecessor + SUCCESSOR_OFFSET);
			}
			successor = UNSAFE.getLong(predecessor + SUCCESSOR_OFFSET);

			UNSAFE.putLong(predecessor + SUCCESSOR_OFFSET, address);
			UNSAFE.putLong(successor + PREDECESSOR_OFFSET, address);

			UNSAFE.putLong(address + PREDECESSOR_OFFSET, predecessor);
			UNSAFE.putLong(address + SUCCESSOR_OFFSET, successor);
		}
		m_size++;

		ret = address + DATA_OFFSET;

		return ret;
	}

	/**
	 * Removes the element at the given psoition from the list
	 * @param p_position
	 *            the position of the element
	 */
	@Override
	protected void remove(final int p_position) {
		long address;
		long predecessor;
		long successor;

		if (p_position < 0 || p_position >= m_size) {
			throw new IndexOutOfBoundsException();
		} else if (p_position == 0) {
			address = m_head;
			m_head = UNSAFE.getLong(address + SUCCESSOR_OFFSET);

			if (m_head != 0) {
				UNSAFE.putLong(m_head + PREDECESSOR_OFFSET, 0);
			} else {
				m_tail = 0;
			}
		} else if (p_position == m_size - 1) {
			address = m_tail;
			m_tail = UNSAFE.getLong(address + PREDECESSOR_OFFSET);

			if (m_tail != 0) {
				UNSAFE.putLong(m_head + SUCCESSOR_OFFSET, 0);
			} else {
				m_head = 0;
			}
		} else {
			predecessor = m_head;
			for (int i = 0; i < p_position; i++) {
				predecessor = UNSAFE.getLong(predecessor + SUCCESSOR_OFFSET);
			}
			address = UNSAFE.getLong(predecessor + SUCCESSOR_OFFSET);
			successor = UNSAFE.getLong(address + SUCCESSOR_OFFSET);

			UNSAFE.putLong(predecessor + SUCCESSOR_OFFSET, successor);
			UNSAFE.putLong(successor + PREDECESSOR_OFFSET, predecessor);
		}
		UNSAFE.freeMemory(address);

		m_size--;
	}

	/**
	 * Creates a sublist with the elements between p_from and p_to
	 * @param p_from
	 *            the from position
	 * @param p_to
	 *            the to position
	 * @return the sublist
	 */
	@Override
	protected UnsafeLinkedList subList(final int p_from, final int p_to) {
		UnsafeLinkedList ret;
		long address;
		long newAddress;
		int elementSize;

		if (p_from < 0 || p_from >= m_size || p_to < 0 || p_to >= m_size || p_from > p_to) {
			throw new IndexOutOfBoundsException();
		}

		ret = new UnsafeLinkedList(getElementSize());

		address = m_head;
		for (int i = 0; i < p_from; i++) {
			address = UNSAFE.getLong(address + SUCCESSOR_OFFSET);
		}

		elementSize = getElementSize();
		for (int i = p_from; i < p_to; i++) {
			newAddress = ret.addTail();
			UNSAFE.copyMemory(address + DATA_OFFSET, newAddress + DATA_OFFSET, elementSize);
		}

		return ret;
	}

	/**
	 * Clears the list
	 */
	@Override
	protected void clear() {
		long current;
		long successor;

		current = m_head;
		while (current != 0) {
			successor = UNSAFE.getLong(current + SUCCESSOR_OFFSET);

			UNSAFE.freeMemory(current);

			current = successor;
		}

		m_head = 0;
		m_tail = 0;

		m_size = 0;
	}

}

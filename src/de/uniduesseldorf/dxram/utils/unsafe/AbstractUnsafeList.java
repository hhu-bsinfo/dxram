
package de.uniduesseldorf.dxram.utils.unsafe;

import sun.misc.Unsafe;

/**
 * Doubly-linked List using the unsafe class to allocate elements
 * @author Florian Klein
 *         04.07.2014
 */
public abstract class AbstractUnsafeList {

	// Constants
	protected static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();

	// Attributes
	private int m_elementSize;

	// Constructors
	/**
	 * Creates an instance of AbstractUnsafeList
	 * @param p_elementSize
	 *            the elementSize to set
	 */
	protected AbstractUnsafeList(final int p_elementSize) {
		m_elementSize = p_elementSize;
	}

	// Getters
	/**
	 * Gets the size of an element
	 * @return the elementSize
	 */
	protected final int getElementSize() {
		return m_elementSize;
	}

	/**
	 * Sets the size of an element
	 * @param p_elementSize
	 *            the elementSize to set
	 */
	protected final void setElementSize(final int p_elementSize) {
		m_elementSize = p_elementSize;
	}

	// Methods
	/**
	 * Get the size
	 * @return the size
	 */
	protected abstract int getSize();

	/**
	 * Get the element at the given position
	 * @param p_position
	 *            the position of the element
	 * @return the element
	 */
	protected abstract long get(final int p_position);

	/**
	 * Get the first element
	 * @return the first element
	 */
	protected final long getFirst() {
		return get(0);
	}

	/**
	 * Get the last element
	 * @return the last element
	 */
	protected final long getLast() {
		return get(getSize() - 1);
	}

	/**
	 * Adds an element to the list at the head
	 * @return the address of the element
	 */
	protected final long addHead() {
		return put(0);
	}

	/**
	 * Adds an element to the list at the tail
	 * @return the address of the element
	 */
	protected final long addTail() {
		return put(getSize());
	}

	/**
	 * Adds an element to the list at the given position. The element at the given position ist pushed back.
	 * @param p_position
	 *            the position to put the element
	 * @return the address of the element
	 */
	protected abstract long put(final int p_position);

	/**
	 * Removes the element at the given psoition from the list
	 * @param p_position
	 *            the position of the element
	 */
	protected abstract void remove(final int p_position);

	/**
	 * Checks if the list contains a defined element
	 * @param p_checker
	 *            the ElementChecker to check the list elements
	 * @return true if the element is contained, false otherwise
	 */
	protected final boolean contains(final ElementChecker p_checker) {
		boolean ret = false;

		for (int i = 0;i < getSize();i++) {
			if (p_checker.check(get(i))) {
				ret = true;

				break;
			}
		}

		return ret;
	}

	/**
	 * Get the first index of the given element
	 * @param p_checker
	 *            the ElementChecker to check the list elements
	 * @return the first index of the given element
	 */
	protected final int indexOf(final ElementChecker p_checker) {
		int ret = -1;

		for (int i = 0;i < getSize();i++) {
			if (p_checker.check(get(i))) {
				ret = i;

				break;
			}
		}

		return ret;
	}

	/**
	 * Get the last index of the given element
	 * @param p_checker
	 *            the ElementChecker to check the list elements
	 * @return the last index of the given element
	 */
	protected final int lastIndexOf(final ElementChecker p_checker) {
		int ret = -1;

		for (int i = getSize() - 1;i >= 0;i--) {
			if (p_checker.check(get(i))) {
				ret = i;

				break;
			}
		}

		return ret;
	}

	/**
	 * Creates a sublist with the elements between p_from and p_to
	 * @param p_from
	 *            the from position
	 * @param p_to
	 *            the to position
	 * @return the sublist
	 */
	protected abstract AbstractUnsafeList subList(final int p_from, final int p_to);

	/**
	 * Clears the list
	 */
	protected abstract void clear();

	/**
	 * Get an iterato for the list
	 * @return the iterator
	 */
	protected final UnsafeListIterator getIterator() {
		return new UnsafeListIterator();
	}

	/**
	 * Get an iterato for the list
	 * @param p_position
	 *            the start position
	 * @return the iterator
	 */
	protected final UnsafeListIterator getIterator(final int p_position) {
		return new UnsafeListIterator(p_position);
	}

	/**
	 * Creates an array list
	 * @return the array list
	 */
	public static UnsafeArrayList createArrayList() {
		return new UnsafeArrayList();
	}

	/**
	 * Creates an array list
	 * @param p_initialSize
	 *            the initial array size
	 * @return the array list
	 */
	public static UnsafeArrayList createArrayList(final int p_initialSize) {
		return new UnsafeArrayList(p_initialSize);
	}

	/**
	 * Creates a linked list
	 * @return the linked list
	 */
	public static UnsafeLinkedList createLinkedList() {
		return new UnsafeLinkedList();
	}

	// Classes
	/**
	 * Iterates over list elements
	 * @author Florian Klein
	 *         04.07.2014
	 */
	protected final class UnsafeListIterator {

		// Attributes
		private int m_position;

		// Constructors
		/**
		 * Creates an instance of ListIterator
		 */
		private UnsafeListIterator() {
			this(-1);
		}

		/**
		 * Creates an instance of ListIterator
		 * @param p_position
		 *            the start position
		 */
		private UnsafeListIterator(final int p_position) {
			m_position = p_position;
		}

		// Methods
		/**
		 * Checks if another element exist
		 * @return true if another element exist, false otherwise
		 */
		protected boolean hasNext() {
			return m_position + 1 < getSize();
		}

		/**
		 * Get the next element
		 * @return the next element
		 */
		protected long getNext() {
			long ret = 0;

			if (hasNext()) {
				m_position++;
				ret = get(m_position);
			}

			return ret;
		}

		/**
		 * Removes the current element
		 */
		protected void remove() {
			AbstractUnsafeList.this.remove(m_position);
		}

		/**
		 * Checks if another element exist
		 * @return true if another element exist, false otherwise
		 */
		public boolean hasPrevious() {
			return m_position - 1 >= 0;
		}

		/**
		 * Get the previous element
		 * @return the previous element
		 */
		public long getPrevious() {
			long ret = 0;

			if (hasPrevious()) {
				m_position--;
				ret = get(m_position);
			}

			return ret;
		}

		/**
		 * Get the next position
		 * @return the next position
		 */
		public int nextIndex() {
			return m_position + 1;
		}

		/**
		 * Get the previous position
		 * @return the previous position
		 */
		public int previousIndex() {
			return m_position - 1;
		}

		/**
		 * Get the current element
		 * @return the current element
		 */
		public long getCurrent() {
			return get(m_position);
		}

		/**
		 * Adds an element
		 * @return the address of the element
		 */
		public long add() {
			return put(m_position + 1);
		}

	}

	/**
	 * Checks an address for a searched element
	 * @author Florian Klein
	 *         04.07.2014
	 */
	protected interface ElementChecker {

		// Methods
		/**
		 * Checks if the searched element is at the given address
		 * @param p_address
		 *            the address
		 * @return true if the searched element is found, false otherwise
		 */
		boolean check(final long p_address);

	}

}

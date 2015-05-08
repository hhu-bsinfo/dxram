
package de.uniduesseldorf.dxram.utils.unsafe;

import java.util.List;

import de.uniduesseldorf.dxram.utils.unsafe.AbstractUnsafeList.ElementChecker;

/**
 * An UnsafeList for integers
 * @author Florian Klein
 *         04.07.2014
 */
@SuppressWarnings("restriction")
public final class LongList extends AbstractList<Long> {

	// Constants
	private static final int ELEMENT_SIZE = 8;

	// Attributes
	private final AbstractUnsafeList m_list;

	// Constructors
	/**
	 * Creates an instance of LongList
	 */
	public LongList() {
		this(AbstractUnsafeList.createArrayList());
	}

	/**
	 * Creates an instance of IntegerList
	 * @param p_list
	 *            the list to use
	 */
	public LongList(final AbstractUnsafeList p_list) {
		super(ELEMENT_SIZE, p_list);

		m_list = getList();
	}

	// Methods
	@Override
	protected Long read(final long p_address) {
		return UNSAFE.getLong(p_address);
	}

	@Override
	protected void write(final long p_address, final Long p_element) {
		UNSAFE.putLong(p_address, p_element);
	}

	@Override
	public List<Long> subList(final int p_fromIndex, final int p_toIndex) {
		return new LongList(m_list.subList(p_fromIndex, p_toIndex));
	}

	/**
	 * Checks if the list contains the given element
	 * @param p_element
	 *            the elements
	 * @return true if the element is contained, false otherwise
	 */
	public boolean containsElement(final long p_element) {
		boolean ret;

		ret = m_list.contains(new LongChecker(p_element));

		return ret;
	}

	/**
	 * Adds an element
	 * @param p_element
	 *            the element
	 */
	public void addElement(final long p_element) {
		long address;

		address = m_list.addTail();
		UNSAFE.putLong(address, p_element);
	}

	/**
	 * Adds the elements
	 * @param p_elements
	 *            the elements
	 */
	public void addElements(final long... p_elements) {
		for (long element : p_elements) {
			addElement(element);
		}
	}

	/**
	 * Adds an element at the given index
	 * @param p_index
	 *            the index
	 * @param p_element
	 *            the element
	 */
	public void addElement(final int p_index, final long p_element) {
		long address;

		address = m_list.put(p_index);
		UNSAFE.putLong(address, p_element);
	}

	/**
	 * Set the element at the given index
	 * @param p_index
	 *            the index
	 * @param p_element
	 *            the element
	 * @return the old element
	 */
	public long setElement(final int p_index, final long p_element) {
		long ret;
		long address;

		address = m_list.get(p_index);
		ret = UNSAFE.getLong(address);
		UNSAFE.putLong(address, p_element);

		return ret;
	}

	/**
	 * Gets the element at the given index
	 * @param p_index
	 *            the index
	 * @return the element
	 */
	public long getElement(final int p_index) {
		long ret;
		long address;

		address = m_list.get(p_index);
		ret = UNSAFE.getLong(address);

		return ret;
	}

	/**
	 * Get the first index of the given element
	 * @param p_element
	 *            the element
	 * @return the first index of the given element
	 */
	public int indexOfElement(final long p_element) {
		int ret;

		ret = m_list.indexOf(new LongChecker(p_element));

		return ret;
	}

	/**
	 * Get the last index of the given element
	 * @param p_element
	 *            the element
	 * @return the last index of the given element
	 */
	public int lastIndexOfElement(final long p_element) {
		int ret;

		ret = m_list.lastIndexOf(new LongChecker(p_element));

		return ret;
	}

	/**
	 * Removes an element
	 * @param p_element
	 *            the element
	 * @return
	 */
	public void removeElement(final long p_element) {
		int index;

		index = m_list.indexOf(new LongChecker(p_element));
		if (index >= 0) {
			m_list.remove(index);
		}
	}

	/**
	 * Removes all element
	 * @param p_element
	 *            the element
	 * @return
	 */
	public void removeElements(final long p_element) {
		int index;

		index = m_list.indexOf(new LongChecker(p_element));
		while (index >= 0) {
			m_list.remove(index);

			index = m_list.indexOf(new LongChecker(p_element));
		}
	}

	/**
	 * Removes the element at the given index
	 * @param p_index
	 *            the index
	 * @return the element
	 */
	public long removeElementAt(final int p_index) {
		long ret;
		long address;

		address = m_list.get(p_index);
		ret = UNSAFE.getLong(address);
		m_list.remove(p_index);

		return ret;
	}

	// Classes
	/**
	 * Checks an address for a searched long
	 * @author Florian Klein
	 *         04.07.2014
	 */
	private final class LongChecker implements ElementChecker {

		// Attributes
		private final long m_value;

		// Constructors
		/**
		 * Creates an instance of IntegerChecker
		 * @param p_value
		 *            the searched value
		 */
		private LongChecker(final long p_value) {
			m_value = p_value;
		}

		// Methods
		@Override
		public boolean check(final long p_address) {
			return m_value == UNSAFE.getInt(p_address);
		}

	}

}

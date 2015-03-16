
package de.uniduesseldorf.dxram.utils.unsafe;

import java.util.List;

import de.uniduesseldorf.dxram.utils.unsafe.AbstractUnsafeList.ElementChecker;

/**
 * An UnsafeList for integers
 * @author Florian Klein
 *         04.07.2014
 */
public final class IntegerList extends AbstractList<Integer> {

	// Constants
	private static final int ELEMENT_SIZE = 4;

	// Attributes
	private final AbstractUnsafeList m_list;

	// Constructors
	/**
	 * Creates an instance of IntegerList
	 */
	public IntegerList() {
		this(AbstractUnsafeList.createArrayList());
	}

	/**
	 * Creates an instance of IntegerList
	 * @param p_list
	 *            the list to use
	 */
	public IntegerList(final AbstractUnsafeList p_list) {
		super(ELEMENT_SIZE, p_list);

		m_list = getList();
	}

	// Methods
	@Override
	protected Integer read(final long p_address) {
		return UNSAFE.getInt(p_address);
	}

	@Override
	protected void write(final long p_address, final Integer p_element) {
		UNSAFE.putInt(p_address, p_element);
	}

	@Override
	public List<Integer> subList(final int p_fromIndex, final int p_toIndex) {
		return new IntegerList(m_list.subList(p_fromIndex, p_toIndex));
	}

	/**
	 * Checks if the list contains the given element
	 * @param p_element
	 *            the elements
	 * @return true if the element is contained, false otherwise
	 */
	public boolean containsElement(final int p_element) {
		boolean ret;

		ret = m_list.contains(new IntegerChecker(p_element));

		return ret;
	}

	/**
	 * Adds an element
	 * @param p_element
	 *            the element
	 */
	public void addElement(final int p_element) {
		long address;

		address = m_list.addTail();
		UNSAFE.putInt(address, p_element);
	}

	/**
	 * Adds the elements
	 * @param p_elements
	 *            the elements
	 */
	public void addElements(final int... p_elements) {
		for (int element : p_elements) {
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
	public void addElement(final int p_index, final int p_element) {
		long address;

		address = m_list.put(p_index);
		UNSAFE.putInt(address, p_element);
	}

	/**
	 * Set the element at the given index
	 * @param p_index
	 *            the index
	 * @param p_element
	 *            the element
	 * @return the old element
	 */
	public int setElement(final int p_index, final int p_element) {
		int ret;
		long address;

		address = m_list.get(p_index);
		ret = UNSAFE.getInt(address);
		UNSAFE.putInt(address, p_element);

		return ret;
	}

	/**
	 * Gets the element at the given index
	 * @param p_index
	 *            the index
	 * @return the element
	 */
	public int getElement(final int p_index) {
		int ret;
		long address;

		address = m_list.get(p_index);
		ret = UNSAFE.getInt(address);

		return ret;
	}

	/**
	 * Get the first index of the given element
	 * @param p_element
	 *            the element
	 * @return the first index of the given element
	 */
	public int indexOfElement(final int p_element) {
		int ret;

		ret = m_list.indexOf(new IntegerChecker(p_element));

		return ret;
	}

	/**
	 * Get the last index of the given element
	 * @param p_element
	 *            the element
	 * @return the last index of the given element
	 */
	public int lastIndexOfElement(final int p_element) {
		int ret;

		ret = m_list.lastIndexOf(new IntegerChecker(p_element));

		return ret;
	}

	/**
	 * Removes an element
	 * @param p_element
	 *            the element
	 * @return
	 */
	public void removeElement(final int p_element) {
		int index;

		index = m_list.indexOf(new IntegerChecker(p_element));
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
	public void removeElements(final int p_element) {
		int index;

		index = m_list.indexOf(new IntegerChecker(p_element));
		while (index >= 0) {
			m_list.remove(index);

			index = m_list.indexOf(new IntegerChecker(p_element));
		}
	}

	/**
	 * Removes the element at the given index
	 * @param p_index
	 *            the index
	 * @return the element
	 */
	public int removeElementAt(final int p_index) {
		int ret;
		long address;

		address = m_list.get(p_index);
		ret = UNSAFE.getInt(address);
		m_list.remove(p_index);

		return ret;
	}

	// Classes
	/**
	 * Checks an address for a searched integer
	 * @author Florian Klein
	 *         04.07.2014
	 */
	private final class IntegerChecker implements ElementChecker {

		// Attributes
		private final int m_value;

		// Constructors
		/**
		 * Creates an instance of IntegerChecker
		 * @param p_value
		 *            the searched value
		 */
		private IntegerChecker(final int p_value) {
			m_value = p_value;
		}

		// Methods
		@Override
		public boolean check(final long p_address) {
			return m_value == UNSAFE.getInt(p_address);
		}

	}

}

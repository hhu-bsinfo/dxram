
package de.uniduesseldorf.dxram.utils.unsafe;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import sun.misc.Unsafe;

import de.uniduesseldorf.dxram.utils.Contract;
import de.uniduesseldorf.dxram.utils.unsafe.AbstractUnsafeList.ElementChecker;
import de.uniduesseldorf.dxram.utils.unsafe.AbstractUnsafeList.UnsafeListIterator;

/**
 * An UnsafeList for generic types
 * @param <Type>
 *            the element type
 * @author Florian Klein
 *         04.07.2014
 */
public abstract class AbstractList<Type> implements List<Type> {

	// Constants
	protected static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();

	// Attributes
	private final AbstractUnsafeList m_list;

	// Constructors
	/**
	 * Creates an instance of IntegerList
	 * @param p_elementSize
	 *            the size of an element
	 */
	public AbstractList(final int p_elementSize) {
		this(p_elementSize, AbstractUnsafeList.createArrayList());
	}

	/**
	 * Creates an instance of IntegerList
	 * @param p_elementSize
	 *            the size of an element
	 * @param p_list
	 *            the list to use
	 */
	public AbstractList(final int p_elementSize, final AbstractUnsafeList p_list) {
		Contract.check(p_elementSize > 0, "invalid element size given");

		m_list = p_list;
		m_list.setElementSize(p_elementSize);
	}

	// Getters
	/**
	 * Gets the list
	 * @return the list
	 */
	protected final AbstractUnsafeList getList() {
		return m_list;
	}

	// Methods
	/**
	 * Reads an element from the given address
	 * @param p_address
	 *            the address
	 * @return the element
	 */
	protected abstract Type read(final long p_address);

	/**
	 * Write an element at the given address
	 * @param p_address
	 *            the address
	 * @param p_element
	 *            the element
	 */
	protected abstract void write(final long p_address, final Type p_element);

	@Override
	public abstract List<Type> subList(final int p_fromIndex, final int p_toIndex);

	@Override
	public final int size() {
		return m_list.getSize();
	}

	@Override
	public final boolean isEmpty() {
		return m_list.getSize() == 0;
	}

	@Override
	public final boolean contains(final Object p_object) {
		boolean ret = false;

		if (p_object != null) {
			ret = m_list.contains(new DefaultChecker(p_object));
		}

		return ret;
	}

	@Override
	public final Iterator<Type> iterator() {
		return listIterator();
	}

	@Override
	public final Object[] toArray() {
		Object[] ret;
		int position;

		ret = new Object[m_list.getSize()];
		position = 0;
		for (Type value : this) {
			ret[position++] = value;
		}

		return ret;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <T> T[] toArray(final T[] p_array) {
		T[] ret;
		int size;
		int position;

		Contract.checkNotNull(p_array, "no array given");

		size = m_list.getSize();
		if (p_array.length >= size) {
			ret = p_array;
		} else {
			ret = (T[])Array.newInstance(p_array.getClass().getComponentType(), size);
		}

		position = 0;
		for (Type value : this) {
			ret[position++] = (T)value;
		}

		return ret;
	}

	@Override
	public final boolean add(final Type p_element) {
		long address;

		Contract.checkNotNull(p_element, "no element given");

		address = m_list.addTail();
		write(address, p_element);

		return true;
	}

	@Override
	public final boolean remove(final Object p_object) {
		boolean ret = false;
		int index;

		if (p_object != null) {
			index = m_list.indexOf(new DefaultChecker(p_object));
			if (index >= 0) {
				m_list.remove(index);

				ret = true;
			}
		}

		return ret;
	}

	@Override
	public final boolean containsAll(final Collection<?> p_collection) {
		boolean ret = true;

		Contract.checkNotNull(p_collection, "no collection given");

		for (Object object : p_collection) {
			if (!contains(object)) {
				ret = false;

				break;
			}
		}

		return ret;
	}

	@Override
	public final boolean addAll(final Collection<? extends Type> p_collection) {
		Contract.checkNotNull(p_collection, "no collection given");

		for (Type value : p_collection) {
			add(value);
		}

		return true;
	}

	@Override
	public final boolean addAll(final int p_index, final Collection<? extends Type> p_collection) {
		int index;
		long address;

		Contract.checkNotNull(p_collection, "no collection given");

		index = p_index;
		for (Type value : p_collection) {
			address = m_list.put(index++);
			write(address, value);
		}

		return true;
	}

	@Override
	public final boolean removeAll(final Collection<?> p_collection) {
		boolean ret = false;

		Contract.checkNotNull(p_collection, "no collection given");

		for (Object object : p_collection) {
			ret |= remove(object);
		}

		return ret;
	}

	@Override
	public final boolean retainAll(final Collection<?> p_collection) {
		boolean ret = false;

		Contract.checkNotNull(p_collection, "no collection given");

		for (final Iterator<Type> iterator = iterator();iterator.hasNext();) {
			if (!p_collection.contains(iterator.next())) {
				iterator.remove();

				ret = true;
			}
		}

		return ret;
	}

	@Override
	public final void clear() {
		m_list.clear();
	}

	@Override
	public final Type get(final int p_index) {
		return read(m_list.get(p_index));
	}

	@Override
	public final Type set(final int p_index, final Type p_element) {
		Type ret;
		long address;

		Contract.checkNotNull(p_element, "no element given");

		address = m_list.get(p_index);
		ret = read(address);
		write(address, p_element);

		return ret;
	}

	@Override
	public final void add(final int p_index, final Type p_element) {
		Contract.checkNotNull(p_element, "no element given");

		write(m_list.put(p_index), p_element);
	}

	@Override
	public final Type remove(final int p_index) {
		Type ret;
		long address;

		address = m_list.get(p_index);
		ret = read(address);
		m_list.remove(p_index);

		return ret;
	}

	@Override
	public final int indexOf(final Object p_object) {
		int ret = -1;

		if (p_object != null) {
			ret = m_list.indexOf(new DefaultChecker(p_object));
		}

		return ret;
	}

	@Override
	public final int lastIndexOf(final Object p_object) {
		int ret = -1;

		if (p_object != null) {
			ret = m_list.lastIndexOf(new DefaultChecker(p_object));
		}

		return ret;
	}

	@Override
	public final ListIterator<Type> listIterator() {
		return new DefaultListIterator(m_list.getIterator());
	}

	@Override
	public final ListIterator<Type> listIterator(final int p_index) {
		return new DefaultListIterator(m_list.getIterator(p_index));
	}

	// Classes
	/**
	 * Iterator for an UnsafeList with integers
	 * @author Florian Klein
	 *         04.07.2014
	 */
	private final class DefaultListIterator implements ListIterator<Type> {

		// Attributes
		private final UnsafeListIterator m_iterator;

		// Constructors
		/**
		 * Creates an instance of IntegerIterator
		 * @param p_iterator
		 *            the iterator for the list
		 */
		private DefaultListIterator(final UnsafeListIterator p_iterator) {
			m_iterator = p_iterator;
		}

		// Methods
		@Override
		public boolean hasNext() {
			return m_iterator.hasNext();
		}

		@Override
		public Type next() {
			return read(m_iterator.getNext());
		}

		@Override
		public void remove() {
			m_iterator.remove();
		}

		@Override
		public boolean hasPrevious() {
			return m_iterator.hasPrevious();
		}

		@Override
		public Type previous() {
			return read(m_iterator.getPrevious());
		}

		@Override
		public int nextIndex() {
			return m_iterator.nextIndex();
		}

		@Override
		public int previousIndex() {
			return m_iterator.previousIndex();
		}

		@Override
		public void set(final Type p_element) {
			Contract.checkNotNull(p_element, "no element given");

			write(m_iterator.getCurrent(), p_element);
		}

		@Override
		public void add(final Type p_element) {
			Contract.checkNotNull(p_element, "no element given");

			write(m_iterator.add(), p_element);
		}

	}

	/**
	 * Checks an address for a searched element
	 * @author Florian Klein
	 *         04.07.2014
	 */
	private final class DefaultChecker implements ElementChecker {

		// Attributes
		private final Object m_value;

		// Constructors
		/**
		 * Creates an instance of Checker
		 * @param p_value
		 *            the searched value
		 */
		private DefaultChecker(final Object p_value) {
			m_value = p_value;
		}

		// Methods
		@Override
		public boolean check(final long p_address) {
			return m_value.equals(read(p_address));
		}

	}

}

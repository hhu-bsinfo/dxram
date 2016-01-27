
package de.hhu.bsinfo.utils.unsafe;

import java.util.List;

import de.hhu.bsinfo.utils.unsafe.AbstractKeyValueList.KeyValuePair;

/**
 * An UnsafeList for key-value pairs
 * @author Florian Klein
 *         05.07.2014
 * @param <KeyType>
 *            the type of the key
 * @param <ValueType>
 *            the type of the value
 */
public abstract class AbstractKeyValueList<KeyType, ValueType> extends AbstractList<KeyValuePair<KeyType, ValueType>> {

	// Attributes
	private final int m_valueOffset;

	// Constructors
	/**
	 * Creates an instance of KeyValueList
	 * @param p_keySize
	 *            the size of a key
	 * @param p_valueSize
	 *            the size of a value
	 */
	public AbstractKeyValueList(final int p_keySize, final int p_valueSize) {
		this(p_keySize, p_valueSize, AbstractUnsafeList.createArrayList());
	}

	/**
	 * Creates an instance of KeyValueList
	 * @param p_keySize
	 *            the size of a key
	 * @param p_valueSize
	 *            the size of a value
	 * @param p_list
	 *            the list to use
	 */
	public AbstractKeyValueList(final int p_keySize, final int p_valueSize, final AbstractUnsafeList p_list) {
		super(p_keySize + p_valueSize, p_list);

		assert p_keySize > 0;
		assert p_valueSize > 0;

		m_valueOffset = p_keySize;
	}

	// Methods
	/**
	 * Reads the key at the given address
	 * @param p_address
	 *            the address
	 * @return the key
	 */
	protected abstract KeyType readKey(final long p_address);

	/**
	 * Reads the value at the given address
	 * @param p_address
	 *            the address
	 * @return the value
	 */
	protected abstract ValueType readValue(final long p_address);

	/**
	 * Writes the key to the given address
	 * @param p_address
	 *            the address
	 * @param p_key
	 *            the key
	 */
	protected abstract void writeKey(final long p_address, final KeyType p_key);

	/**
	 * Writes the value to the given address
	 * @param p_address
	 *            the address
	 * @param p_value
	 *            the value
	 */
	protected abstract void writeValue(final long p_address, final ValueType p_value);

	@Override
	public abstract List<KeyValuePair<KeyType, ValueType>> subList(final int p_fromIndex, final int p_toIndex);

	@Override
	protected final KeyValuePair<KeyType, ValueType> read(final long p_address) {
		return new KeyValuePair<>(readKey(p_address), readValue(p_address + m_valueOffset));
	}

	@Override
	protected final void write(final long p_address, final KeyValuePair<KeyType, ValueType> p_element) {
		writeKey(p_address, p_element.getKey());
		writeValue(p_address + m_valueOffset, p_element.getValue());
	}

	/**
	 * Adds a key-value pair
	 * @param p_key
	 *            the key
	 * @param p_value
	 *            the value
	 */
	public final void add(final KeyType p_key, final ValueType p_value) {
		add(new KeyValuePair<>(p_key, p_value));
	}

	/**
	 * Adds a key-value pair at the given index
	 * @param p_index
	 *            the index
	 * @param p_key
	 *            the key
	 * @param p_value
	 *            the value
	 */
	public final void add(final int p_index, final KeyType p_key, final ValueType p_value) {
		add(p_index, new KeyValuePair<>(p_key, p_value));
	}

	/**
	 * Sets the key-value pair at the given index
	 * @param p_index
	 *            the index
	 * @param p_key
	 *            the key
	 * @param p_value
	 *            the value
	 * @return the old key-value pair
	 */
	public final KeyValuePair<KeyType, ValueType> set(final int p_index, final KeyType p_key, final ValueType p_value) {
		return set(p_index, new KeyValuePair<>(p_key, p_value));
	}

	/**
	 * Determines the first index of the key-value pair with the given key
	 * @param p_key
	 *            the key
	 * @return the first index
	 */
	public final int indexOfKey(final KeyType p_key) {
		return indexOf(new KeyValuePair<>(p_key));
	}

	/**
	 * Determines the last index of the key-value pair with the given key
	 * @param p_key
	 *            the key
	 * @return the last index
	 */
	public final int lastIndexOfKey(final KeyType p_key) {
		return lastIndexOf(new KeyValuePair<>(p_key));
	}

	/**
	 * Removes the key-value pair with the given key
	 * @param p_key
	 *            the key
	 * @return true if a key-value pair is removed, false otherwise
	 */
	public final boolean removeKey(final KeyType p_key) {
		return remove(new KeyValuePair<>(p_key));
	}

	/**
	 * Checks if the list contains a key-value pair with the given key
	 * @param p_key
	 *            the key
	 * @return true if a key-value pair is contained, false otherwise
	 */
	public final boolean containsKey(final KeyType p_key) {
		return contains(new KeyValuePair<>(p_key));
	}

	// Classes
	/**
	 * Represents a key-value pair
	 * @author Florian Klein
	 *         05.07.2014
	 * @param <KeyType>
	 *            the type of the key
	 * @param <ValueType>
	 *            the type of the value
	 */
	public static class KeyValuePair<KeyType, ValueType> {

		// Attributes
		private final KeyType m_key;
		private final ValueType m_value;

		// Constructors
		/**
		 * Creates an instance of KeyValuePair
		 * @param p_key
		 *            the key
		 */
		public KeyValuePair(final KeyType p_key) {
			assert p_key != null;

			m_key = p_key;
			m_value = null;
		}

		/**
		 * Creates an instance of KeyValuePair
		 * @param p_key
		 *            the key
		 * @param p_value
		 *            the value
		 */
		public KeyValuePair(final KeyType p_key, final ValueType p_value) {
			assert p_key != null;
			assert p_value != null;

			m_key = p_key;
			m_value = p_value;
		}

		/**
		 * Get the key
		 * @return the key
		 */
		public final KeyType getKey() {
			return m_key;
		}

		/**
		 * Get the value
		 * @return the value
		 */
		public final ValueType getValue() {
			return m_value;
		}

		// Methods
		@Override
		public final boolean equals(final Object p_object) {
			boolean ret = false;

			if (p_object instanceof KeyValuePair) {
				ret = m_key.equals(((KeyValuePair<?, ?>) p_object).getKey());
			}

			return ret;
		}

		@Override
		public final int hashCode() {
			return m_key.hashCode();
		}

	}

}


package de.uniduesseldorf.dxram.utils.unsafe;

import java.util.List;

/**
 * An UnsafeList for key-value pairs with an Integer key and a Long value
 * @author Florian Klein
 *         05.07.2014
 */
@SuppressWarnings("restriction")
public final class IntegerLongList extends AbstractKeyValueList<Integer, Long> {

	// Constants
	private static final int KEY_SIZE = 4;
	private static final int VALUE_SIZE = 8;

	// Constructors
	/**
	 * Creates an instance of IntegerLongList
	 */
	public IntegerLongList() {
		this(AbstractUnsafeList.createArrayList());
	}

	/**
	 * Creates an instance of IntegerLongList
	 * @param p_list
	 *            the list to use
	 */
	public IntegerLongList(final AbstractUnsafeList p_list) {
		super(KEY_SIZE, VALUE_SIZE, p_list);
	}

	// Methods
	@Override
	protected Integer readKey(final long p_address) {
		return UNSAFE.getInt(p_address);
	}

	@Override
	protected Long readValue(final long p_address) {
		return UNSAFE.getLong(p_address);
	}

	@Override
	protected void writeKey(final long p_address, final Integer p_key) {
		UNSAFE.putInt(p_address, p_key);
	}

	@Override
	protected void writeValue(final long p_address, final Long p_value) {
		UNSAFE.putLong(p_address, p_value);
	}

	@Override
	public List<de.uniduesseldorf.dxram.utils.unsafe.AbstractKeyValueList.KeyValuePair<Integer, Long>> subList(final int p_fromIndex, final int p_toIndex) {
		return new IntegerLongList(getList().subList(p_fromIndex, p_toIndex));
	}

}

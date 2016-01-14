
package de.hhu.bsinfo.utils;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Methods for handling arrays
 * @author Florian Klein
 *         20.06.2012
 */
public final class ArrayTools {

	// Constructors
	/**
	 * Creates an instance of ArrayTools
	 */
	private ArrayTools() {}

	// Methods
	/**
	 * Converts a collection to an array
	 * @param p_collection
	 *            the collection
	 * @param p_class
	 *            the class of the elements
	 * @param <T>
	 *            the element type of the collection
	 * @return the array
	 */
	@SuppressWarnings("unchecked")
	public static <T> T[] toArray(final Collection<T> p_collection, final Class<T> p_class) {
		T[] ret;
		int i;

		Contract.checkNotNull(p_collection, "no collection given");
		Contract.checkNotNull(p_class, "no class given");

		ret = (T[]) Array.newInstance(p_class, p_collection.size());
		i = 0;
		for (T element : p_collection) {
			ret[i++] = element;
		}

		return ret;
	}

	/**
	 * Converts a set of arrays to a single one
	 * @param p_class
	 *            the class of the elements
	 * @param p_arrays
	 *            the set of arrays
	 * @param <T>
	 *            the element type of the array
	 * @return the array
	 */
	@SuppressWarnings("unchecked")
	public static <T> T[] concatArrays(final Class<T> p_class, final T[]... p_arrays) {
		T[] ret;
		int length;
		int i;

		Contract.checkNotNull(p_class, "no class given");
		Contract.checkNotNull(p_arrays, "no arrays given");

		length = 0;
		for (T[] array : p_arrays) {
			length += array.length;
		}

		ret = (T[]) Array.newInstance(p_class, length);
		i = 0;
		for (T[] array : p_arrays) {
			for (T element : array) {
				ret[i++] = element;
			}
		}

		return ret;
	}

	/**
	 * Converts a set of arrays to a single one
	 * @param p_arrays
	 *            the set of arrays
	 * @return the array
	 */
	public static byte[] concatArrays(final byte[]... p_arrays) {
		byte[] ret;
		int length;
		int i;

		Contract.checkNotNull(p_arrays, "no arrays given");

		length = 0;
		for (byte[] array : p_arrays) {
			length += array.length;
		}

		ret = new byte[length];
		i = 0;
		for (byte[] array : p_arrays) {
			for (byte element : array) {
				ret[i++] = element;
			}
		}

		return ret;
	}

	/**
	 * Splits an array in a set of arrays
	 * @param p_array
	 *            the array to split
	 * @param p_length
	 *            the length of the new arrays (the last array could be smaller)
	 * @return the set of arrays
	 */
	public static byte[][] splitArray(final byte[] p_array, final int p_length) {
		byte[][] ret;
		int length;
		int array;

		Contract.checkNotNull(p_array, "no array given");

		length = p_array.length / p_length;
		if (p_array.length % p_length != 0) {
			length++;
		}

		ret = new byte[length][];
		array = -1;
		for (int i = 0; i < p_array.length; i++) {
			if (i % p_length == 0) {
				ret[++array] = new byte[p_length];
			}

			ret[array][i % p_length] = p_array[i];
		}

		return ret;
	}

	/**
	 * Copies an array into another
	 * @param p_source
	 *            the source array
	 * @param p_target
	 *            the target array
	 * @return the difference between the target and the source array length
	 */
	public static int copyArray(final byte[] p_source, final byte[] p_target) {
		int ret;

		Contract.checkNotNull(p_source, "no source array given");
		Contract.checkNotNull(p_target, "no target array given");

		ret = p_target.length - p_source.length;

		if (p_source.length > p_target.length) {
			for (int i = 0; i < p_target.length; i++) {
				p_target[i] = p_source[i];
			}
		} else {
			for (int i = 0; i < p_source.length; i++) {
				p_target[i] = p_source[i];
			}
		}

		return ret;
	}
}

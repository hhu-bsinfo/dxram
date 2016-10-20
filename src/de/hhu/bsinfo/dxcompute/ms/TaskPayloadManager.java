package de.hhu.bsinfo.dxcompute.ms;

import javax.management.openmbean.KeyAlreadyExistsException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manager class for task payloads. Make sure to register any newly created
 * task payloads here.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 12.10.16
 */
public final class TaskPayloadManager {

	private static Map<Integer, Class<? extends TaskPayload>> m_registeredTaskClasses = new HashMap<>();

	/**
	 * Static class
	 */
	private TaskPayloadManager() {

	}

	/**
	 * Create an instance of a registered task payload.
	 * Throws RuntimeException If no task payload specifeid by the ids could be created.
	 *
	 * @param p_typeId    Type id of the task payload.
	 * @param p_subtypeId Subtype id of the task payload.
	 * @return New instance of the specified task payload.
	 */
	public static TaskPayload createInstance(final short p_typeId, final short p_subtypeId) {
		Class<? extends TaskPayload> clazz =
				m_registeredTaskClasses.get(((p_typeId & 0xFFFF) << 16) | p_subtypeId);
		if (clazz == null) {
			throw new RuntimeException(
					"Cannot create instance of TaskPayload with id " + p_typeId + "|" + p_subtypeId
							+ ", not registered.");
		}

		try {
			Constructor<? extends TaskPayload> ctor = clazz.getConstructor();
			return ctor.newInstance();
		} catch (final NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			throw new RuntimeException(
					"Cannot create instance of TaskPayload with id " + p_typeId + "|" + p_subtypeId + ": "
							+ e.getMessage());
		}
	}

	public static TaskPayload createInstance(final short p_typeId, final short p_subtypeId, final Object... p_args) {

		Class<? extends TaskPayload> clazz =
				m_registeredTaskClasses.get(((p_typeId & 0xFFFF) << 16) | p_subtypeId);
		if (clazz == null) {
			throw new RuntimeException(
					"Cannot create instance of TaskPayload with id " + p_typeId + "|" + p_subtypeId
							+ ", not registered.");
		}

		// find constructor with correct parameter count
		for (Constructor<?> ctor : clazz.getConstructors()) {

			if (ctor.getParameterCount() == p_args.length) {
				try {
					return (TaskPayload) ctor.newInstance(p_args);
				} catch (final SecurityException | InstantiationException | IllegalAccessException
						| IllegalArgumentException | InvocationTargetException e) {
					throw new RuntimeException(
							"Cannot create instance of TaskPayload with id " + p_typeId + "|" + p_subtypeId + ": "
									+ e.getMessage());
				}
			}
		}

		throw new RuntimeException(
				"Cannot create instance of TaskPayload with id " + p_typeId + "|" + p_subtypeId
						+ ": can not find constructor with parameter count " + p_args.length);
	}

	/**
	 * Get the class that's registered for the specified type and subtype.
	 *
	 * @param p_typeId    Type id of the task payload.
	 * @param p_subtypeId Subtype id of the task payload.
	 * @return Class registered for specified type and subtype or null if non registered
	 */
	public static Class<? extends TaskPayload> getRegisteredClass(final short p_typeId, final short p_subtypeId) {
		return m_registeredTaskClasses.get(((p_typeId & 0xFFFF) << 16) | p_subtypeId);
	}

	/**
	 * Get the list of registered task payload classes.
	 *
	 * @return List of registered task payload classes.
	 */
	public static Map<Integer, Class<? extends TaskPayload>> getRegisteredTaskPayloadClasses() {
		return m_registeredTaskClasses;
	}

	/**
	 * Register a new task payload class.
	 *
	 * @param p_typeId    Type id for the class.
	 * @param p_subtypeId Subtype id for the class.
	 * @param p_class     Class to register for the specified ids.
	 * @throws KeyAlreadyExistsException If the type id and subtype id are already used.
	 */
	public static void registerTaskPayloadClass(final short p_typeId, final short p_subtypeId,
			final Class<? extends TaskPayload> p_class) {
		Class<? extends TaskPayload> clazz =
				m_registeredTaskClasses.put(((p_typeId & 0xFFFF) << 16) | p_subtypeId, p_class);
		if (clazz != null) {
			throw new KeyAlreadyExistsException("Failed registering " + p_class.getSimpleName()
					+ " for " + p_typeId + "/" + p_subtypeId + " failed, key already used");
		}
	}
}

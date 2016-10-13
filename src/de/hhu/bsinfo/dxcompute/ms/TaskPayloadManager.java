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

	private static Map<Integer, Class<? extends AbstractTaskPayload>> m_registeredTaskClasses = new HashMap<>();

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
	public static AbstractTaskPayload createInstance(final short p_typeId, final short p_subtypeId) {
		Class<? extends AbstractTaskPayload> clazz =
				m_registeredTaskClasses.get(((p_typeId & 0xFFFF) << 16) | p_subtypeId);
		if (clazz == null) {
			throw new RuntimeException(
					"Cannot create instance of TaskPayload with id " + p_typeId + "|" + p_subtypeId
							+ ", not registered.");
		}

		try {
			Constructor<? extends AbstractTaskPayload> ctor = clazz.getConstructor();
			return ctor.newInstance();
		} catch (final NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			throw new RuntimeException(
					"Cannot create instance of TaskPayload with id " + p_typeId + "|" + p_subtypeId + ": "
							+ e.getMessage());
		}
	}

	/**
	 * Get the list of registered task payload classes.
	 *
	 * @return List of registered task payload classes.
	 */
	public static Map<Integer, Class<? extends AbstractTaskPayload>> getRegisteredTaskPayloadClasses() {
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
			final Class<? extends AbstractTaskPayload> p_class) {
		Class<? extends AbstractTaskPayload> clazz =
				m_registeredTaskClasses.put(((p_typeId & 0xFFFF) << 16) | p_subtypeId, p_class);
		if (clazz != null) {
			throw new KeyAlreadyExistsException("Failed registering " + p_class.getSimpleName()
					+ " for " + p_typeId + "/" + p_subtypeId + " failed, key already used");
		}
	}
}

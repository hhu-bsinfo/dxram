package de.hhu.bsinfo.dxram.job;

import java.util.HashMap;
import java.util.Map;

public class JobMap {
    private Map<Short, Class<? extends AbstractJob>> m_registeredJobTypes =
            new HashMap<Short, Class<? extends AbstractJob>>();

    public JobMap() {

    }

    /**
     * Register a Job with its type ID. Make sure to do this for every Job subclass you create.
     *
     * @param p_typeID
     *         Type ID for the Job class.
     * @param p_clazz
     *         The class to register for the specified ID.
     * @throws JobRuntimeException
     *         If another Job class was already registered with the specified type ID.
     */
    public void registerType(final short p_typeID, final Class<? extends AbstractJob> p_clazz) {
        Class<? extends AbstractJob> clazz = m_registeredJobTypes.putIfAbsent(p_typeID, p_clazz);

        if (clazz != null) {
            throw new JobRuntimeException("Job type with ID " + p_typeID + " already registered for class " + clazz);
        }
    }

    /**
     * Create an instance of a Job using a previously registered type ID.
     * This is used for serialization/creating Jobs from serialized data.
     *
     * @param p_typeID
     *         Type ID of the job to create.
     * @return Job object.
     * @throws JobRuntimeException
     *         If creating an instance failed or no Job class is registered for the specified type ID.
     */
    public AbstractJob createInstance(final short p_typeID) {
        AbstractJob job;
        Class<? extends AbstractJob> clazz = m_registeredJobTypes.get(p_typeID);

        if (clazz != null) {
            try {
                job = clazz.newInstance();
            } catch (final InstantiationException | IllegalAccessException e) {
                throw new JobRuntimeException("Creating instance for job type ID " + p_typeID + " failed.", e);
            }
        } else {
            throw new JobRuntimeException(
                    "Creating instance for job type ID " + p_typeID + " failed, no class registered for type " +
                            p_typeID);
        }

        return job;
    }
}

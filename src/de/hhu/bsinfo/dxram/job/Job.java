package de.hhu.bsinfo.dxram.job;

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Base class for an job that can be exeucted by the
 * JobService/JobComponent.
 * Jobs can either be used internally by DXRAM, pushed through
 * the JobComponent, or externally by the user, pushed through
 * the JobService.
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public abstract class Job implements Importable, Exportable
{
	private long m_ID = JobID.INVALID_ID;
	private long[] m_parameterChunkIDs = null;

	// nasty, but the only way to get access to services/the API for 
	// external/user code
	private DXRAMServiceAccessor m_serviceAccessor = null;
	
	protected static int MS_JOB_TYPE_ID_COUNTER = 0;
	
	private static Map<Short, Class<? extends Job>> m_registeredJobTypes = new HashMap<Short, Class<? extends Job>>();
	
	/**
	 * Create an instance of a Job using a previously registered type ID.
	 * This is used for serialization/creating Jobs from serialized data.
	 * @param p_typeID Type ID of the job to create.
	 * @return Job object.
	 * @throws JobRuntimeException If creating an instance failed or no Job class is registered for the specified type ID.
	 */
	public static Job createInstance(final short p_typeID)
	{
		Job job = null;
		Class<? extends Job> clazz = m_registeredJobTypes.get(p_typeID);
		
		if (clazz != null)
		{
			try {
				job = clazz.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new JobRuntimeException("Creating instance for job type ID " + p_typeID + " failed.", e);
			}
		} else {
			throw new JobRuntimeException("Creating instance for job type ID " + p_typeID + " failed, no class registered for type " + p_typeID);
		}
		
		return job;
	}
	
	/**
	 * Register a Job with its type ID. Make sure to do this for every Job subclass you create.
	 * @param p_typeID Type ID for the Job class.
	 * @param p_clazz The class to register for the specified ID.
	 * @throws JobRuntimeException If another Job class was already registered with the specified type ID.
	 */
	public static void registerType(final short p_typeID, final Class<? extends Job> p_clazz)
	{
		Class<? extends Job> clazz = m_registeredJobTypes.putIfAbsent(p_typeID, p_clazz);
		if (clazz != null) {
			throw new JobRuntimeException("Job type with ID " + p_typeID + " already registered for class " + clazz);
		}
	}
	
	/**
	 * Constructor
	 * @param p_parameterChunkIDs ChunkIDs, which are passed as parameters to the Job on execution.
	 * 			The max count is 255, which is plenty enough.
	 */
	public Job(final long... p_parameterChunkIDs)
	{
		assert p_parameterChunkIDs.length <= 255;
		assert p_parameterChunkIDs.length >= 0;
		if (p_parameterChunkIDs != null) {
			m_parameterChunkIDs = p_parameterChunkIDs;
		} else {
			m_parameterChunkIDs = new long[0];
		}
	}
	
	/** 
	 * Constructor with no chunkID parameters.
	 */
	public Job()
	{
		m_parameterChunkIDs = new long[0];
	}
	
	/**
	 * Get the type ID of this Job object.
	 * @return Type ID.
	 */
	public abstract short getTypeID();
	
	/**
	 * Get the ID of this job.
	 * @return ID of job (16 bits NodeID/CreatorID + 48 bits local JobID).
	 */
	public long getID()
	{
		return m_ID;
	}
	
	/**
	 * Execute this job.
	 * @param p_nodeID NodeID of the node this job is excecuted on.
	 */
	public void execute(short p_nodeID) {
		execute(p_nodeID, m_parameterChunkIDs);
	}
	
	@Override
	public String toString()
	{
		return "Job[m_ID " + Long.toHexString(m_ID) + "]";
	}
	
	// -------------------------------------------------------------------
	
	@Override
	public int importObject(final Importer p_importer, final int p_size)
	{
		m_ID = p_importer.readLong();
		
		m_parameterChunkIDs = new long[p_importer.readByte() & 0xFF];
		for (int i = 0; i < m_parameterChunkIDs.length; i++)
		{
			m_parameterChunkIDs[i] = p_importer.readLong();
		}
		
		return sizeofObject();
	}
	
	@Override
	public int exportObject(final Exporter p_exporter, final int p_size)
	{
		p_exporter.writeLong(m_ID);
		
		p_exporter.writeByte((byte) (m_parameterChunkIDs.length & 0xFF));
		for (int i = 0; i < m_parameterChunkIDs.length; i++)
		{
			p_exporter.writeLong(m_parameterChunkIDs[i]);
		}
		
		return sizeofObject();
	}
	
	@Override
	public int sizeofObject()
	{
		return Long.BYTES + Byte.BYTES + Long.BYTES * m_parameterChunkIDs.length;
	}
	
	@Override
	public boolean hasDynamicObjectSize()
	{
		return true;
	}
	
	// -------------------------------------------------------------------
	
	/**
	 * Set the ID for this job. Has to be done before scheduling it.
	 * @param p_id ID to set for this job.
	 */
	void setID(final long p_id)
	{
		m_ID = p_id;
	}
	
	/**
	 * Set the service accessor to allow access to all DXRAM services (refer to DXRAMServiceAcessor class for details
	 * and important notes).
	 * @param p_serviceAccessor Service accessor to set.
	 */
	void setServiceAccessor(final DXRAMServiceAccessor p_serviceAccessor)
	{
		m_serviceAccessor = p_serviceAccessor;
	}
	
	// -------------------------------------------------------------------
	
	/**
	 * Implement this function and put your code to be executed with this job here.
	 * @param p_nodeID NodeID this job is executed on.
	 * @param p_chunkIDs Parameters this job was created with.
	 */
	protected abstract void execute(final short p_nodeID, long[] p_chunkIDs);
	
	/**
	 * Get services from DXRAM within this job. This is allowed for external jobs, only.
	 * These jobs are pushed to the system from outside/the user via JobService.
	 * If a job was pushed internally (via the JobComponent) this returns null
	 * as we do not allow access to services internally (breaking isolation concept).
	 * 
	 * @param p_class Class of the service to get. If the service has different implementations, use the common interface
	 * 					or abstract class to get the registered instance.
	 * @return Reference to the service if available and enabled, null otherwise.
	 */
	protected <T extends DXRAMService> T getService(final Class<T> p_class)
	{
		if (m_serviceAccessor != null)
			return m_serviceAccessor.getService(p_class);
		else
			return null;
	}
}
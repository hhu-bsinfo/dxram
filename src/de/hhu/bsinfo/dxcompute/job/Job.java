package de.hhu.bsinfo.dxcompute.job;

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

public abstract class Job implements Importable, Exportable
{
	private long m_ID = JobID.INVALID_ID;
	private long[] m_parameterChunkIDs;

	private JobDelegate m_jobDelegate = null;
	
	protected static int MS_JOB_TYPE_ID_COUNTER = 0;
	
	private static Map<Short, Class<? extends Job>> m_registeredJobTypes = new HashMap<Short, Class<? extends Job>>();
	
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
		}
		
		return job;
	}
	
	public static void registerType(final short p_typeID, final Class<? extends Job> p_clazz)
	{
		Class<? extends Job> clazz = m_registeredJobTypes.putIfAbsent(p_typeID, p_clazz);
		if (clazz != null) {
			throw new JobRuntimeException("Job type with ID " + p_typeID + " already registered for class " + clazz);
		}
	}
	
	// allow max 255 longs, because that's plenty enough...should work with storing
	// data as chunks instead of passing it as values or sending across the network
	public Job(final long... p_parameterChunkIDs)
	{
		assert p_parameterChunkIDs.length <= 255;
		assert p_parameterChunkIDs.length >= 0;
		m_parameterChunkIDs = p_parameterChunkIDs;
	}
	
	public abstract short getTypeID();
	
	public long getID()
	{
		return m_ID;
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
		return Short.BYTES + Long.BYTES + Long.BYTES * m_parameterChunkIDs.length;
	}
	
	@Override
	public boolean hasDynamicObjectSize()
	{
		return true;
	}
	
	// -------------------------------------------------------------------
	
	void setID(final long p_id)
	{
		m_ID = p_id;
	}
	
	void setJobDelegate(final JobDelegate p_jobDelegate)
	{
		m_jobDelegate = p_jobDelegate;
	}
	
	// -------------------------------------------------------------------
	
	protected abstract void execute(final short p_nodeID);
	
	protected boolean pushJobPublicRemoteQueue(final Job p_job, final short p_nodeID)
	{
		return m_jobDelegate.pushJobPublicRemoteQueue(p_job, p_nodeID);
	}
	
	protected boolean pushJobPublicLocalQueue(final Job p_job)
	{
		return m_jobDelegate.pushJobPublicLocalQueue(p_job);
	}
	
	protected boolean pushJobPrivateQueue(final Job p_job)
	{
		return m_jobDelegate.pushJobPrivateQueue(p_job);
	}
	
	protected void log(final String p_msg)
	{
		m_jobDelegate.log(this, p_msg);
	}
}
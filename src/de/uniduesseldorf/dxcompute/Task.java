package de.uniduesseldorf.dxcompute;

public abstract class Task 
{
	private TaskInterface m_taskInterface;
	private StorageInterface m_storageInterface;
	
	public Task()
	{
		
	}
	
	public Object execute(final TaskInterface p_taskInterface, final StorageInterface p_storageInterface, final Object p_arg)
	{
		m_taskInterface = p_taskInterface;
		m_storageInterface = p_storageInterface;
		return execute(p_arg);
	}
	
	protected abstract Object execute(final Object p_arg);
	
	protected TaskInterface getTaskInterface()
	{
		return m_taskInterface;
	}
	
	protected StorageInterface getStorageInterface()
	{
		return m_storageInterface;
	}
}

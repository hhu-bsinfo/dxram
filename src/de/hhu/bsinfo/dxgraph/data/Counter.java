package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

public class Counter implements DataStructure
{	
	private long m_id = -1;
	private long m_counter = 0;

	public Counter()
	{

	}
	
	public Counter(final long p_id)
	{
		m_id = p_id;
	}
	
	public void setCounter(final long p_counter) {
		m_counter = p_counter;
	}
	
	public long getCounter() {
		return m_counter;
	}
	
	public void incrementCounter(final long p_val) {
		m_counter += p_val;
	}
	
	// -----------------------------------------------------------------------------

	@Override
	public long getID()
	{
		return m_id;
	}
	
	@Override
	public void setID(final long p_id)
	{
		m_id = p_id;
	}

	@Override
	public int importObject(Importer p_importer, int p_size) {
		m_counter = p_importer.readLong();
		
		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return Long.BYTES;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return true;
	}

	@Override
	public int exportObject(Exporter p_exporter, int p_size) {
		p_exporter.writeLong(m_counter);
		
		return sizeofObject();
	}
	
	@Override
	public String toString()
	{
		return "Counter["+ Long.toHexString(m_id) + "]: " + m_counter;
	}
}
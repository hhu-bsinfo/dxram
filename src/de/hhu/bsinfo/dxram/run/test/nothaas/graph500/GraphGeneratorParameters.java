package de.hhu.bsinfo.dxram.run.test.nothaas.graph500;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

public class GraphGeneratorParameters implements DataStructure {

	private long m_id = -1;
	// default values, toy size from graph500
	private byte m_graphScale = 26;
	private float m_edgeFactor = 16;
	// default values from kronecker graph generator from graph500
	// A + B + C + D = 1.0
	private float m_probabilityA = 0.57f;
	private float m_probabilityB = 0.19f;
	private float m_probabilityC = 0.19f;
	private float m_probabilityD = 0.05f;
	
	public GraphGeneratorParameters(final long p_id)
	{
		m_id = p_id;
	}
	
	public byte getGraphScale()
	{
		return m_graphScale;
	}
	
	public float getEdgeFactor()
	{
		return m_edgeFactor;
	}
	
	public float getProbabilityA()
	{
		return m_probabilityA;
	}
	
	public float getProbabilityB()
	{
		return m_probabilityB;
	}
	
	public float getProbabilityC()
	{
		return m_probabilityC;
	}
	
	public float getProbabilityD()
	{
		return m_probabilityD;
	}
	
	public long getVertexCount()
	{
		return (long) Math.pow(2, m_graphScale);
	}
	
	public long getEdgeCount()
	{
		return (long) (m_edgeFactor * getVertexCount());
	}
	
	
	
	@Override
	public int importObject(Importer p_importer, int p_size) {
		m_graphScale = p_importer.readByte();
		m_edgeFactor = p_importer.readFloat();
		m_probabilityA = p_importer.readFloat();
		m_probabilityB = p_importer.readFloat();
		m_probabilityC = p_importer.readFloat();
		m_probabilityD = p_importer.readFloat();
		
		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return Byte.BYTES + Float.BYTES * 5;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return false;
	}

	@Override
	public int exportObject(Exporter p_exporter, int p_size) {
		p_exporter.writeByte(m_graphScale);
		p_exporter.writeFloat(m_edgeFactor);
		p_exporter.writeFloat(m_probabilityA);
		p_exporter.writeFloat(m_probabilityB);
		p_exporter.writeFloat(m_probabilityC);
		p_exporter.writeFloat(m_probabilityD);
		
		return sizeofObject();
	}

	@Override
	public long getID() {
		return m_id;
	}

}

package de.hhu.bsinfo.dxgraph.gen;

public class GraphGeneratorKronecker extends GraphGenerator {

	private byte m_graphScale = 26;
	private float m_edgeFactor = 16;
	// default values from kronecker graph generator from graph500
	// A + B + C + D = 1.0
	private float m_probabilityA = 0.57f;
	private float m_probabilityB = 0.19f;
	private float m_probabilityC = 0.19f;
	private float m_probabilityD = 0.05f;
	
	public GraphGeneratorKronecker()
	{
		
	}
	
	@Override
	public boolean generate(int p_numNodes) 
	{
		// TODO Auto-generated method stub
		return false;
	}
}

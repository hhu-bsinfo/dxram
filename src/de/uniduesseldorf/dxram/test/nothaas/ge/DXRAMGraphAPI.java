package de.uniduesseldorf.dxram.test.nothaas.ge;

import java.util.Vector;

import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

public interface DXRAMGraphAPI 
{		
	long createNode() throws DXRAMException;
	
	long createNode(long... p_edges) throws DXRAMException;
	
	long createNode(int p_userData, long... p_edges) throws DXRAMException;
	
	long addEdges(long p_sourceNodeID, long... p_destinationNodeIDs) throws DXRAMException;
	
	Vector<Long> getEdges(long p_nodeID) throws DXRAMException;
	
	boolean deleteEdge(long p_sourceNodeID, long p_destinationNodeID) throws DXRAMException;
	
	boolean deleteNode(long p_nodeID) throws DXRAMException;
	
	int getUserData(long p_nodeID) throws DXRAMException;
	
	void setUserData(long p_nodeID, int p_userData) throws DXRAMException;
}
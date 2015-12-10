package de.uniduesseldorf.dxram.core.log;

import com.sun.corba.se.impl.ior.GenericTaggedComponent;

import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.mem.Chunk;

public class LogComponent extends DXRAMComponent implements LogInterface
{

	public LogComponent(String p_componentIdentifier, int p_priorityInit, int p_priorityShutdown) {
		super("Log", p_priorityInit, p_priorityShutdown);

	}

	// ---------------------------------------------------------------------------------
	
	@Override
	public void initBackupRange(long p_firstChunkIDOrRangeID, short[] p_backupPeers) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Chunk[] recoverBackupRange(short p_owner, long p_chunkID, byte p_rangeID) throws DXRAMException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Chunk[] recoverBackupRangeFromFile(String p_fileName, String p_path) throws DXRAMException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void printBackupRange(short p_owner, long p_chunkID, byte p_rangeID) throws DXRAMException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public short getAproxHeaderSize(short p_nodeID, long p_localID, int p_size) {
		// TODO Auto-generated method stub
		return 0;
	}

	// ---------------------------------------------------------------------------------
	
	@Override
	protected boolean initComponent() {
		getCom
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean shutdownComponent() {
		// TODO Auto-generated method stub
		return false;
	}

}

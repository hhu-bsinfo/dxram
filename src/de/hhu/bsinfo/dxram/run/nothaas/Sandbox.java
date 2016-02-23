package de.hhu.bsinfo.dxram.run.nothaas;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.event.Event;
import de.hhu.bsinfo.dxram.lookup.event.NodeFailureEvent;
import de.hhu.bsinfo.soh.HeapWalker;
import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.soh.StorageUnsafeMemory;

public class Sandbox {

	public static void main(String[] args)
	{		
		System.out.println(Long.parseLong("-C181000000000001", 16));
	}
}

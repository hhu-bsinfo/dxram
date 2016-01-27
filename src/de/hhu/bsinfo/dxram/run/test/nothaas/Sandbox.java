package de.hhu.bsinfo.dxram.run.test.nothaas;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.event.Event;
import de.hhu.bsinfo.dxram.lookup.event.NodeFailureEvent;
import de.hhu.bsinfo.soh.HeapWalker;
import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.soh.StorageUnsafeMemory;

public class Sandbox {

	public static void main(String[] args)
	{		
		SmallObjectHeap heap = new SmallObjectHeap(new StorageUnsafeMemory());
		heap.initialize(1024, 1024);
		
		System.out.println(heap.malloc(981));
		
		System.out.println(HeapWalker.walk(heap));
	}
}

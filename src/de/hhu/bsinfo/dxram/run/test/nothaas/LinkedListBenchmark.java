package de.hhu.bsinfo.dxram.run.test.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.Stopwatch;
import de.hhu.bsinfo.utils.main.Main;
import de.hhu.bsinfo.utils.main.MainArguments;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

public class LinkedListBenchmark extends Main
{
	public static final Pair<String, Integer> ARG_ITEM_COUNT = new Pair<String, Integer>("itemCount", 100);
	
	private DXRAM m_dxram = null;
	private ChunkService m_chunkService = null;
	private Stopwatch m_stopwatch = new Stopwatch();
	
	public static void main(final String[] args) {
		Main main = new LinkedListBenchmark();
		main.run(args);
	}
	
	public LinkedListBenchmark()
	{
		m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf");
		m_chunkService = m_dxram.getService(ChunkService.class);
	}
	
	@Override
	protected void registerDefaultProgramArguments(MainArguments p_arguments) {
		p_arguments.setArgument(ARG_ITEM_COUNT);
	}

	@Override
	protected int main(MainArguments p_arguments) {
		final int itemCount = p_arguments.getArgument(ARG_ITEM_COUNT);
		
		System.out.println("Creating linked list with " + itemCount + " items.");
		m_stopwatch.start();
		long listHead = createLinkedList(itemCount);
		m_stopwatch.printAndStop();
		System.out.println("Done creating linked list.");
		
		System.out.println("Walking linked list, head " + listHead);
		m_stopwatch.start();
		long itemsTouched = walkLinkedList(listHead);
		m_stopwatch.printAndStop();
		System.out.println("Walking linked list done, total elements touched: " + itemsTouched);
		
		System.out.println("Done");
		return 0;
	}
	
	private long createLinkedList(int numItems)
	{	
		LinkedListElement[] chunks = new LinkedListElement[numItems];
		long[] chunkIDs = m_chunkService.create(8, numItems);
		LinkedListElement head = null;
		LinkedListElement previousChunk = null;
		
		for (int i = 0; i < chunkIDs.length; i++)
		{
			chunks[i] = new LinkedListElement(chunkIDs[i]);
			if (previousChunk == null)
			{
				// init head
				head = chunks[i];
				previousChunk = head;
			} else {
				previousChunk.setNextID(chunks[i].getID());
				previousChunk = chunks[i];
			}
		}
		
		// mark end
		chunks[chunks.length - 1].setNextID(-1);
		
		if (m_chunkService.put(chunks) != chunks.length)
		{
			System.out.println("Putting linked list failed.");
			return -1;
		}
		
		return head.getID();
	}
	
	private long walkLinkedList(long headChunkID)
	{	
		long counter = 0;
		LinkedListElement chunk = new LinkedListElement(headChunkID);
		if (m_chunkService.get(chunk) != 1)
		{
			System.out.println("Getting head chunk if linked list failed.");
			return 0;
		}
		counter++;
		
		while (chunk != null)
		{
			long nextChunkID = chunk.getNextID();
			if (nextChunkID == -1)
				break;
			// reuse same chunk to avoid allocations
			chunk.setOwnID(nextChunkID);
			m_chunkService.get(chunk);
			counter++;
		}
		
		return counter;
	}
	
	private static class LinkedListElement implements DataStructure
	{
		private long m_ownID = -1;
		private long m_nextID = -1;
		
		public LinkedListElement(final long p_ownID)
		{
			m_ownID = p_ownID;
		}
		
		public void setOwnID(final long p_ownID) {
			m_ownID = p_ownID;
		}
		
		public void setNextID(final long p_nextID) {
			m_nextID = p_nextID;
		}
		
		public long getNextID() {
			return m_nextID;
		}
		
		@Override
		public int importObject(Importer p_importer, int p_size) {
			m_nextID = p_importer.readLong();
			
			return Long.BYTES;
		}

		@Override
		public int sizeofObject() {
			return Long.BYTES;
		}

		@Override
		public boolean hasDynamicObjectSize() {
			return false;
		}

		@Override
		public int exportObject(Exporter p_exporter, int p_size) {
			p_exporter.writeLong(m_nextID);
			return Long.BYTES;
		}

		@Override
		public long getID() {
			return m_ownID;
		}
		
	}
}

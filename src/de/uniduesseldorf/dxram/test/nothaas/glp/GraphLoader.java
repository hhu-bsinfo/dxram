package de.uniduesseldorf.dxram.test.nothaas.glp;

import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import de.uniduesseldorf.dxram.utils.Pair;

public class GraphLoader 
{
	protected Vector<GraphReader> m_readers = new Vector<GraphReader>();
	protected Vector<GraphImporter> m_importers = new Vector<GraphImporter>();
	private NodeMapping m_nodeMapping = null;
	private ExecutorService m_executor = null;
	
	public GraphLoader(int p_threadPoolSize)
	{
		m_executor = Executors.newFixedThreadPool(p_threadPoolSize);
	}
	
	public boolean addReader(GraphReader p_reader)
	{
		// check if same instance already exists
		for (GraphReader reader : m_readers)
		{
			if (reader == p_reader)
				return false;
		}
		
		m_readers.add(p_reader);
		return true;
	}
	
	public boolean addImporter(GraphImporter p_importer)
	{
		// check if same instance already exists
		for (GraphImporter importer : m_importers)
		{
			if (importer == p_importer)
				return false;
		}
		
		m_importers.add(p_importer);
		return true;
	}
	
	public void setNodeMapping(NodeMapping p_nodeMappingInstance)
	{
		m_nodeMapping = p_nodeMappingInstance;
	}
	
	public NodeMapping getNodeMapping()
	{
		return m_nodeMapping;
	}
	
	public boolean execute()
	{
		if (m_nodeMapping == null)
		{
			System.out.println("Error, no node mapping set.");
			return false;
		}
		if (m_readers.size() < 1)
		{
			System.out.println("No readers added.");
			return false;
		}
		if (m_importers.size() < 1)
		{
			System.out.println("No importers added.");
			return false;
		}
		
		Vector<Worker> tasks = createTasksQueue();
		Vector<Future<?>> submitedTasks = new Vector<Future<?>>();
		for (Worker worker : tasks)
		{
			submitedTasks.add(m_executor.submit(worker));
		}
		
		System.out.println("Waiting for workers to finish...");
		
		for (Future<?> future : submitedTasks)
		{
			try
			{
				future.get();
			}
			catch (final ExecutionException | InterruptedException e)
			{
				e.printStackTrace();
			}
		}
		
		m_executor.shutdown();
		
		System.out.println("Workers finished.");
		
		return true;
	}
	
	private Vector<Worker> createTasksQueue()
	{
		Vector<Worker> queue = new Vector<Worker>();
		
		if (m_readers.size() < m_importers.size())
		{
			final int numReaderInstances = m_importers.size() / m_readers.size(); 
			int remainder = m_importers.size() % m_readers.size();
			
			int workerInstance = 0;
			int readerIdx = 0;
			int importerIdx = 0;
			while (readerIdx < m_readers.size())
			{	
				int totalReaderInstances = numReaderInstances;
				if (remainder > 0)
				{
					totalReaderInstances++;
					remainder--;
				}
				
				for (int readInst = 0; readInst < totalReaderInstances; readInst++)
				{
					ReaderInstance readerInstance = new ReaderInstance(readInst, totalReaderInstances, m_readers.get(readerIdx));
					ImporterInstance importerInstance = new ImporterInstance(0, 1, m_importers.get(importerIdx));
				
					queue.add(new Worker(workerInstance++, readerInstance, importerInstance));
					importerIdx++;
				}
				
				readerIdx++;
			}
		}
		else if (m_readers.size() > m_importers.size())
		{
			final int numImporterInstances = m_importers.size() / m_readers.size(); 
			int remainder = m_importers.size() % m_readers.size();
			
			int workerInstance = 0;
			int readerIdx = 0;
			int importerIdx = 0;
			while (importerIdx < m_importers.size())
			{	
				int totalImporterInstances = numImporterInstances;
				if (remainder > 0)
				{
					totalImporterInstances++;
					remainder--;
				}
				
				for (int importerInst = 0; importerInst < totalImporterInstances; importerInst++)
				{
					ReaderInstance readerInstance = new ReaderInstance(0, 1, m_readers.get(readerIdx));
					ImporterInstance importerInstance = new ImporterInstance(importerInst, totalImporterInstances, m_importers.get(importerIdx));
				
					queue.add(new Worker(workerInstance++, readerInstance, importerInstance));
					readerIdx++;
				}
				
				importerIdx++;
			}
		}
		else
		{
			for (int i = 0; i < m_readers.size(); i++)
			{
				ReaderInstance readerInstance = new ReaderInstance(i, m_readers.size(), m_readers.get(i));
				ImporterInstance importerInstance = new ImporterInstance(i, m_importers.size(), m_importers.get(i));
				
				queue.add(new Worker(i, readerInstance, importerInstance));
			}
		}
		
		Iterator<Worker> it = queue.iterator();
		while (it.hasNext())
		{
			Worker work = it.next();
			System.out.println(work);
		}
		
		return queue;
	}
	
	private class ReaderInstance 
	{
		private int m_instanceID = -1;
		private int m_totalInstances = -1;
		private GraphReader m_reader = null;
		
		public ReaderInstance(int p_instanceID, int p_totalInstances, GraphReader p_reader)
		{
			m_instanceID = p_instanceID;
			m_totalInstances = p_totalInstances;
			m_reader = p_reader;
		}
		
		public int getInstanceID()
		{
			return m_instanceID;
		}
		
		public int getTotalInstances()
		{
			return m_totalInstances;
		}
		
		public int readEdges(Vector<Pair<Long, Long>> p_buffer, int p_count)
		{
			return m_reader.readEdges(m_instanceID, m_totalInstances, p_buffer, p_count);
		}
	}
	
	private class ImporterInstance
	{
		private int m_instanceID = -1;
		private int m_totalInstances = -1;
		private GraphImporter m_importer = null;
		
		public ImporterInstance(int p_instanceID, int p_totalInstances, GraphImporter p_importer)
		{
			m_instanceID = p_instanceID;
			m_totalInstances = p_totalInstances;
			m_importer = p_importer;
		}
		
		public int getInstanceID()
		{
			return m_instanceID;
		}
		
		public int getTotalInstances()
		{
			return m_totalInstances;
		}
		
		boolean addEdge(long p_nodeFrom, long p_nodeTo, NodeMapping p_nodeMapping)
		{
			return m_importer.addEdge(m_instanceID, m_totalInstances, p_nodeFrom, p_nodeTo, p_nodeMapping);
		}
	}
	
	private class Worker implements Runnable
	{
		private int m_workerInstance = -1;
		private ReaderInstance m_reader = null;
		private ImporterInstance m_importer = null;
		
		public Worker(int p_workerInstance, ReaderInstance p_readerInstance, ImporterInstance p_importerInstance)
		{
			m_workerInstance = p_workerInstance;
			m_reader = p_readerInstance;
			m_importer = p_importerInstance;
		}
		
		@Override
		public void run() 
		{
			final int edgeCount = 100;
			Vector<Pair<Long, Long>> buffer = new Vector<Pair<Long, Long>>();
			int readEdges = 0;
			
			do
			{				
				readEdges = m_reader.readEdges(buffer, edgeCount);
				if (readEdges <= 0)
					break;
				
				for (Pair<Long, Long> edge : buffer)
				{
					m_importer.addEdge(edge.first(), edge.second(), m_nodeMapping);
				}
				
				buffer.clear();
			}
			while (readEdges > 0);
			
			System.out.println("Worker (" + m_workerInstance + ") finished.");
		}
		
		@Override
		public String toString()
		{
			return "Worker: Reader(" + m_reader.getInstanceID() + "/" + (m_reader.getTotalInstances() - 1) + ") -> " +
					"Importer(" + m_importer.getInstanceID() + "/" + (m_importer.getTotalInstances() - 1) + ")";
		}
	}
}

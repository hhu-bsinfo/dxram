package de.uniduesseldorf.utils.conf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ConfigurationTest 
{
	static class Config
	{
	    // Hier schreibst du deine Attribute hin
	    public String TITLE;
	    public int WIDTH;
	    public int HEIGHT;
	    public double RATIO;
	    public ArrayList<String> NAMES;

	    public Config() {
	        // Hier die Standardwerte der Attribute, falls diese
	        // nicht in der Konfigurationsdatei vorhanden sind. 
	           this.TITLE = "Titel der Anwendung";
	        this.WIDTH = 800;
	        this.HEIGHT = 600;
	        this.RATIO = 0.6;
	        this.NAMES = new ArrayList<String>();
	        this.NAMES.add("Peter");
	        this.NAMES.add("Paul");
	    }
	}
	
	static class DXRAMEngine
	{
		static class Component
		{
			public boolean m_enabled = true;
			public String m_class = new String();
		}
		
		static class Service
		{
			public boolean m_enabled = true;
			public String m_class = new String();
		}
		
		public int m_nodeID = 0;
		public ArrayList<Component> m_components = new ArrayList<Component>();
		public ArrayList<Service> m_services = new ArrayList<Service>();
	}
	
	static class ComponentMemoryManager extends DXRAMEngine.Component
	{
		public long m_ramSize = 1024 * 1024 * 124 * 4;
		public long m_segmentSize = 1024 * 1024 * 1024;
	}
	
	static class ComponentLookup extends DXRAMEngine.Component
	{
		public int m_sleep = 1;
	}
	
	public static void main(String[] args) 
	{	
		//createConfiguration();
		//readConfiguration();
		toFile(new File("test.json"), new Config());
		DXRAMEngine dxram = fromFile(new File("test.json"));
		
		System.out.println("components");
		for (DXRAMEngine.Component comp : dxram.m_components)
		{
			System.out.println(comp.m_class);
		}
	}
	
	public static void toFile(File file, Config config) 
	{
		DXRAMEngine bla = new DXRAMEngine();
		
		bla.m_components.add(new ComponentMemoryManager());
		bla.m_components.add(new ComponentLookup());
		
		GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.setPrettyPrinting().create();
        String jsonConfig = gson.toJson(bla);
        FileWriter writer;
        try {
            writer = new FileWriter(file);
            writer.write(jsonConfig);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	private static DXRAMEngine fromFile(File configFile) {
	        try {
	            Gson gson = new GsonBuilder().setPrettyPrinting().create();
	            BufferedReader reader = new BufferedReader(new InputStreamReader(
	new FileInputStream(configFile)));
	            return gson.fromJson(reader, DXRAMEngine.class);
	        } catch (FileNotFoundException e) {
	            return null;
	        }
	}
	
	public static void createConfiguration() 
	{
		Configuration config = new Configuration("NewTest");
		config.AddValue("/Resolution/X", 1024);
		config.AddValue("/Resolution/Y", 768);
		config.AddValue("/Services/Service/Name", 0, "my_service");
		config.AddValue("/Services/Service/IP", 0, "192.168.178.20");
		config.AddValue("/Services/Service/Port", 0, "1234");
		config.AddValue("/Services/Service/Name", 1, "your_service");
		config.AddValue("/Services/Service/IP", 1, "192.168.178.55");
		config.AddValue("/Services/Service/Port", 1, "4321");
		config.AddValue("/Services/Service/Name", 2, "his_service");
		config.AddValue("/Services/Service/IP", 2, "192.168.178.11");
		config.AddValue("/Services/Service/Port", 2, "9999");
		
		System.out.println("=================================");
		System.out.println(config);
		System.out.println("=================================");
		
		ConfigurationXMLLoaderFile fileLoader = new ConfigurationXMLLoaderFile("test.conf");
		ConfigurationXMLParser parser = new ConfigurationXMLParser(fileLoader);
		try {
			parser.writeConfiguration(config);
		} catch (ConfigurationException e) {
			System.out.println("Writing configuration failed: " + e);
			System.exit(-1);
		}
	}
	
	public static void readConfiguration()
	{
		Configuration config = new Configuration("ReadTest");
		ConfigurationXMLLoaderFile fileLoader = new ConfigurationXMLLoaderFile("test.conf");
		ConfigurationXMLParser parser = new ConfigurationXMLParser(fileLoader);
		try {
			parser.readConfiguration(config);
		} catch (ConfigurationException e) {
			System.out.println("Reading configuration failed: " + e);
			System.exit(-1);
		}
		
		System.out.println("=================================");
		System.out.println(config);
		System.out.println("=================================");
	}
}

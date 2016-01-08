package de.uniduesseldorf.utils.conf;

public class ConfigurationTest 
{	
	public static void main(String[] args) 
	{	
		//createConfiguration("test.conf");
		//readConfiguration("test.conf");
		readConfiguration("config/dxram.conf");
	}
	
	public static void createConfiguration(final String p_path) 
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
		
		ConfigurationXMLLoaderFile fileLoader = new ConfigurationXMLLoaderFile(p_path);
		ConfigurationXMLParser parser = new ConfigurationXMLParser(fileLoader);
		try {
			parser.writeConfiguration(config);
		} catch (ConfigurationException e) {
			System.out.println("Writing configuration failed: " + e);
			System.exit(-1);
		}
	}
	
	public static void readConfiguration(final String p_path)
	{
		Configuration config = new Configuration("ReadTest");
		ConfigurationXMLLoaderFile fileLoader = new ConfigurationXMLLoaderFile(p_path);
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

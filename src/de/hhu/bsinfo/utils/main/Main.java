package de.hhu.bsinfo.utils.main;

public abstract class Main 
{
	private MainArguments m_arguments = new MainArguments();
	private MainArgumentsParser m_argumentsParser = new DefaultMainArgumentsParser();
	
	public Main() {
		
	}
	
	public Main(final MainArgumentsParser p_argumentsParser) {
		m_argumentsParser = p_argumentsParser;
	}
	
	public void run(final String[] args) {
		registerDefaultProgramArguments(m_arguments);
		m_argumentsParser.parseArguments(args, m_arguments);
		
		int exitCode = main(m_arguments);
		
		System.exit(exitCode);
	}
	
	protected abstract void registerDefaultProgramArguments(final MainArguments p_arguments);
	
	protected abstract int main(final MainArguments p_arguments);
}

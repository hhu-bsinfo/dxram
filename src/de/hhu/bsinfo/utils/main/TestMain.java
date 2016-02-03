package de.hhu.bsinfo.utils.main;

import de.hhu.bsinfo.utils.Pair;

/**
 * Example/Test implementation to show usage of the main application framwork.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class TestMain extends Main {

	public static final Pair<String, Integer> PARAM_1 = new Pair<String, Integer>("param1", 10);
	public static final Pair<String, String> PARAM_2 = new Pair<String, String>("param2", "test");
	public static final Pair<String, Long> PARAM_3 = new Pair<String, Long>("param3", 1000L);
	
	/**
	 * Java main function.
	 * @param args Argument list.
	 */
	public static void main(final String[] args) {
		Main main = new TestMain();
		main.run(args);
	}

	/**
	 * Constructor
	 */
	public TestMain() {
		
	}
	
	@Override
	protected void registerDefaultProgramArguments(MainArguments p_arguments) {
		p_arguments.setArgument(PARAM_1);
		p_arguments.setArgument(PARAM_2);
		p_arguments.setArgument(PARAM_3);
	}

	@Override
	protected int main(final MainArguments p_arguments) {
		System.out.println(p_arguments);
		
		return 0;
	}
}

package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public class TerminalCommandPrint extends TerminalCommand{
	
	private static final Argument MS_ARG_MSG = new Argument("msg", null, false, "Message to print");
	
	@Override
	public String getName() {
		return "print";
	}
	
	@Override
	public String getDescription() {
		return "Print a message";
	}
	
	@Override
	public void registerArguments(final ArgumentList p_arguments)
	{
		p_arguments.setArgument(MS_ARG_MSG);
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		String msg = p_arguments.getArgumentValue(MS_ARG_MSG, String.class);
		System.out.println(msg);
		return true;
	}
	
}

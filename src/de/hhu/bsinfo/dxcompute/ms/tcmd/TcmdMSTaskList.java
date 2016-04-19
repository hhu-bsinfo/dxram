
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdMSTaskList extends AbstractTerminalCommand {
	@Override
	public String getName() {
		return "comptasklist";
	}

	@Override
	public String getDescription() {
		return "List all available tasks that are registered and can be executed";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {

	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Map<Integer, Class<? extends AbstractTaskPayload>> map = AbstractTaskPayload.getRegisteredTaskPayloadClasses();
		System.out.println("Registered task payload classes (" + map.size() + "): ");
		for (Entry<Integer, Class<? extends AbstractTaskPayload>> entry : map.entrySet()) {
			System.out.println(entry.getValue().getSimpleName() + ": " + (entry.getKey() >> 16 & 0xFFFF) + ", "
					+ (entry.getKey() & 0xFFFF));
		}

		return true;
	}
}

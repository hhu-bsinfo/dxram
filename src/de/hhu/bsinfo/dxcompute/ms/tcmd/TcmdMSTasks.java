
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxcompute.ms.TaskPayloadManager;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Terminal command to get a list of registered tasks allowed for submission.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class TcmdMSTasks extends AbstractTerminalCommand {
	@Override
	public String getName() {
		return "comptasks";
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
		Map<Integer, Class<? extends AbstractTaskPayload>> map = TaskPayloadManager.getRegisteredTaskPayloadClasses();
		getTerminalDelegate().println("Registered task payload classes (" + map.size() + "): ");

		// sort the list by tid and stid
		List<Map.Entry<Integer, Class<? extends AbstractTaskPayload>>> list =
				new LinkedList<>(map.entrySet());
		Collections.sort(list, (p_o1, p_o2) -> {
			if (p_o1.getKey() < p_o2.getKey()) {
				return -1;
			} else if (p_o1.getKey() > p_o2.getKey()) {
				return 1;
			} else {
				return 0;
			}
		});

		for (Entry<Integer, Class<? extends AbstractTaskPayload>> entry : list) {
			getTerminalDelegate()
					.println(entry.getValue().getSimpleName() + ": " + (entry.getKey() >> 16 & 0xFFFF) + ", "
							+ (entry.getKey() & 0xFFFF));
		}

		return true;
	}
}

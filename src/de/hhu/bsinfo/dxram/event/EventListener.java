package de.hhu.bsinfo.dxram.event;

public interface EventListener<T extends Event> {
	void eventTriggered(final T p_event);
}

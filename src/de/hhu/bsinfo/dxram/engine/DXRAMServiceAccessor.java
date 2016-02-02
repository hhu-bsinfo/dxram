package de.hhu.bsinfo.dxram.engine;

// could be called a back door to still get services
// somehow in places it is not possible...
// so far this is only used for Jobs to be able to access services
// as they execute external code. do not use this anywhere else
// if you do not have a good reason i.e. hacks not allowed
public interface DXRAMServiceAccessor {
	public <T extends DXRAMService> T getService(final Class<T> p_class);
}

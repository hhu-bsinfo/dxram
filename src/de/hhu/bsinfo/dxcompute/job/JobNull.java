package de.hhu.bsinfo.dxcompute.job;

public class JobNull extends Job {

	public static final short MS_TYPE_ID = 0;
	static {
		registerType(MS_TYPE_ID, JobNull.class);
	}
	
	public JobNull() {
		super(null);
	}

	@Override
	public short getTypeID()
	{
		return MS_TYPE_ID;
	}
	
	@Override
	protected void execute(short p_nodeID) {
		log("I am null job.");	
	}
}

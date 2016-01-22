package de.hhu.bsinfo.utils.reflect.unit;

public interface UnitConverter
{
	public String getUnitIdentifier();
	
	public Object convert(final Object p_value);
}

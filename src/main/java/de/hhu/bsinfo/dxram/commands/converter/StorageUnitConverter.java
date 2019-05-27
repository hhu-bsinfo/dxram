package de.hhu.bsinfo.dxram.commands.converter;

import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import picocli.CommandLine;

public class StorageUnitConverter implements CommandLine.ITypeConverter<StorageUnit> {

    @Override
    public StorageUnit convert(String value) {
        for(int i = 0; i < value.length(); i++) {
            if(Character.isLetter(value.charAt(i))) {
                return new StorageUnit(Integer.parseInt(value.substring(0, i)), value.substring(i));
            }
        }

        return new StorageUnit(Integer.parseInt(value), StorageUnit.BYTE);
    }
}

package de.hhu.bsinfo.dxram.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.module.Dependency;

public class DependencyInjector implements Injector {

    private static final Logger LOGGER = LogManager.getFormatterLogger(DependencyInjector.class);

    private final DependencyProvider m_dependencyProvider;

    public DependencyInjector(final DependencyProvider p_dependencyProvider) {
        m_dependencyProvider = p_dependencyProvider;
    }

    @Override
    public void inject(final Object p_instance) {
        for (Field injectableField : getDependencyFields(p_instance.getClass())) {
            if (Modifier.isFinal(injectableField.getModifiers())){
                throw new RuntimeException("Injecting final fields is not supported");
            }

            Class type = injectableField.getType();
            Object object = m_dependencyProvider.get(type);

            if (object == null) {
                LOGGER.warn("Could not find dependency %s", type.getCanonicalName());
                continue;
            }

            if (!type.isAssignableFrom(object.getClass())) {
                LOGGER.warn("dependency %s is not compatible with field type %s",
                        object.getClass().getCanonicalName(), type.getCanonicalName());
                continue;
            }

            try {
                injectableField.setAccessible(true);
                injectableField.set(p_instance, object);
            } catch (IllegalAccessException e) {
                throw new InjectionException(String.format("Could not inject %s within %s",
                        injectableField.getName(), object.getClass().getCanonicalName()));
            }
        }
    }

    public static List<Field> getDependencyFields(final Class p_class) {
        return Arrays.stream(p_class.getDeclaredFields())
                .filter(field -> field.getAnnotation(Dependency.class) != null)
                .collect(Collectors.toList());
    }

    public static List<Class> getDependencies(final Class p_class) {
        return getDependencyFields(p_class).stream()
                .map(Field::getType)
                .collect(Collectors.toList());
    }
}

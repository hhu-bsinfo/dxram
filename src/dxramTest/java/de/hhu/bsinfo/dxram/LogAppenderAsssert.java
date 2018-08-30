package de.hhu.bsinfo.dxram;

import java.io.Serializable;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.junit.Assert;

@Plugin(name = "LogAppenderAssert", category = "Core", elementType = "appender", printObject = true)
public class LogAppenderAsssert extends AbstractAppender {
    protected LogAppenderAsssert(final String p_name, final Filter p_filter,
            final Layout<? extends Serializable> p_layout, final boolean p_ignoreExceptions) {
        super(p_name, p_filter, p_layout, p_ignoreExceptions);
    }

    @Override
    public void append(final LogEvent p_event) {
        if (p_event.getLevel().equals(Level.ERROR) || p_event.getLevel().equals(Level.FATAL)) {
            Assert.fail(p_event.getMessage().getFormattedMessage());
        }
    }

    @PluginFactory
    public static LogAppenderAsssert createAppender(
            @PluginAttribute("name")
                    String p_name,
            @PluginElement("Layout")
                    Layout<? extends Serializable> p_layout,
            @PluginElement("Filter")
            final Filter p_filter) {
        if (p_name == null) {
            LOGGER.error("No name provided for MyCustomAppenderImpl");
            return null;
        }

        return new LogAppenderAsssert(p_name, p_filter, p_layout, true);
    }
}

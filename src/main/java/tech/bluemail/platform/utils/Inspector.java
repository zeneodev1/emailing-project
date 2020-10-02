package tech.bluemail.platform.utils;

import java.lang.reflect.Field;
import org.apache.commons.lang.ArrayUtils;
import tech.bluemail.platform.exceptions.SystemException;
import tech.bluemail.platform.logging.Logger;
import tech.bluemail.platform.meta.annotations.Column;

public class Inspector {
    public static String[] classFields(Object object) {
        String[] fields = new String[0];
        if (object != null)
            for (Field field : object.getClass().getDeclaredFields())
                fields = (String[])ArrayUtils.add((Object[])fields, field.getName());
        return fields;
    }

    public static Column columnMeta(Object object, String columnName) {
        if (object != null)
            try {
                Field field = object.getClass().getDeclaredField(columnName);
                Column[] annotations = field.<Column>getAnnotationsByType(Column.class);
                if (annotations.length > 0)
                    return annotations[0];
            } catch (Exception e) {
                Logger.error((Exception)new SystemException(e), Inspector.class);
            }
        return null;
    }
}

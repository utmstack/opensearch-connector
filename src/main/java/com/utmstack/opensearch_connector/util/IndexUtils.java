package com.utmstack.opensearch_connector.util;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.opensearch._types.mapping.Property;

import java.util.Map;

public class IndexUtils {
    private static final String CLASSNAME = "IndexUtils";

    /**
     * This is a recursive method that iterate over the index mappings response,
     * build the fields names and types and fill a map with those values
     *
     * @param mapping A map representing an index mapping
     * @param result  A map to set the operation results
     * @param parent  Name of the parent field
     */
    public static void propertiesFromMapping(Map<String, Property> mapping, Map<String, String> result, String parent) {
        final String ctx = CLASSNAME + ".propertiesFromMapping";
        try {
            mapping.forEach((k, v) -> {
                String key = k;
                if (!StringUtils.isEmpty(parent))
                    key = parent + "." + k;

                if (v.isObject()) {
                    propertiesFromMapping(v.object().properties(), result, key);
                } else {
                    result.put(key, v._kind().jsonValue());
                    if (v.isText()) {
                        Map<String, Property> fields = v.text().fields();
                        if (!MapUtils.isEmpty(fields) && fields.containsKey("keyword"))
                            result.put(key + "." + "keyword",  fields.get("keyword")._kind().jsonValue());
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }
}

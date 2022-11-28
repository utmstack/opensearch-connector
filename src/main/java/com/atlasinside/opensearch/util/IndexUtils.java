package com.atlasinside.opensearch.util;

import com.atlasinside.opensearch.types.IndexPropertyType;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.indices.get_mapping.IndexMappingRecord;

import java.util.Map;

public class IndexUtils {
    private static final String CLASSNAME = "IndexUtils";

    /**
     *
     * @param mapping
     * @param result
     * @param parent
     */
    public static void propertiesFromMapping(Map<String, Property> mapping,
                                             Map<String, IndexPropertyType> result,
                                             String parent) {
        final String ctx = CLASSNAME + ".propertiesFromMapping";

        mapping.forEach((k, v)->{
            String key = k;
            if (StringUtils.isNotBlank(parent))
                key = parent + "." + k;

            if (v.isObject()) {
                propertiesFromMapping(v.object().properties(), result, key);
            } else {
                result.put(key, new IndexPropertyType(key, v._kind().jsonValue()));

                if (v.isText()) {
                    Map<String, Property> fields = v.text().fields();
                    if (MapUtils.isNotEmpty(fields) && fields.containsKey("keyword")) {
                        String keyword = key + "." + "keyword";
                        result.put(keyword, new IndexPropertyType(keyword, fields.get("keyword")._kind().jsonValue()));
                    }
                }
            }
        });
    }
}

package com.utmstack.opensearch_connector.util;

import com.utmstack.opensearch_connector.types.SqlQueryResponse;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlResponseMapper {

    public static List<Map<String, Object>> toKeyValue(SqlQueryResponse response) {
        List<String> columnNames = response.getSchema()
                .stream()
                .map(col -> (col.getAlias() != null && !col.getAlias().isEmpty()) ? col.getAlias() : col.getName())
                .collect(Collectors.toList());

        return response.getDatarows().stream()
                .map(row -> {
                    Map<String, Object> map = new LinkedHashMap<>();
                    for (int i = 0; i < columnNames.size(); i++) {
                        map.put(columnNames.get(i), i < row.size() ? row.get(i) : null);
                    }
                    return map;
                })
                .collect(Collectors.toList());
    }
}


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireSinkRecord;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class EnsureFieldExistsInsert<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String OVERVIEW_DOC =
        "Insert a specified static field into the record if it does not already exist.";

    private interface ConfigName {
        String STATIC_FIELD = "static.field";
        String STATIC_VALUE = "static.value";
    }

    private static final String OPTIONALITY_DOC = "Suffix with <code>!</code> to make this a required field, or <code>?</code> to keep it optional (the default).";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.STATIC_FIELD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                "The name of the field to ensure exists.")
            .define(ConfigName.STATIC_VALUE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                "The value to insert if the field does not exist.");

    private static final String PURPOSE = "field insertion";

    private String staticFieldName;
    private String staticFieldValue;

    private Cache<Schema, Schema> schemaUpdateCache;


    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        staticFieldName = config.getString(ConfigName.STATIC_FIELD);
        staticFieldValue = config.getString(ConfigName.STATIC_VALUE);

        if (staticFieldName == null || staticFieldValue == null) {
            throw new ConfigException("Configuration for static field name and value is required.");
        }
    
        // Initialize your cache for schema updates if needed.
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }


    @Override
    public R apply(R record) {

        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {

        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);

        if (!updatedValue.containsKey(staticFieldName)) {
            updatedValue.put(staticFieldName, staticFieldValue);
        }

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
    
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null && value.schema().field(staticFieldName) == null) {
            // Start building a new schema based on the existing one
            SchemaBuilder builder = SchemaUtil.copySchemaBasics(value.schema(), SchemaBuilder.struct().name(value.schema().name()));
            for (Field field : value.schema().fields()) {
                builder = builder.field(field.name(), field.schema());
            }
            // Add the new field
            builder = builder.field(staticFieldName, Schema.OPTIONAL_STRING_SCHEMA);
            updatedSchema = builder.build();
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }
    
        // Create a new Struct with the updated schema and copy values
        Struct updatedValue = new Struct(updatedSchema != null ? updatedSchema : value.schema());
        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        if (updatedSchema != null) {
            updatedValue.put(staticFieldName, staticFieldValue);
        }
    
        return newRecord(record, updatedSchema != null ? updatedSchema : value.schema(), updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    
        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }
    
        // Add the static field if it's not already present
        if (!schema.fields().stream().anyMatch(f -> f.name().equals(staticFieldName))) {
            builder.field(staticFieldName, Schema.OPTIONAL_STRING_SCHEMA);
        }
    
        return builder.build();
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends EnsureFieldExistsInsert<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }
    
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }
    
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }
    
    public static class Value<R extends ConnectRecord<R>> extends EnsureFieldExistsInsert<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }
    
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
    
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }    

}

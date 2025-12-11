package com.example.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;

public class DateTimeOffsetToTimestamp<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String CONF_CONVERT_ALL = "convert.all.datetimeoffset";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONF_CONVERT_ALL, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
                    "Convert all Debezium ZonedTimestamp fields (datetimeoffset) to Kafka Timestamp.");

    private boolean convertAll;

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.convertAll = config.getBoolean(CONF_CONVERT_ALL);
    }

    @Override
    public R apply(R record) {
        if (!(record.value() instanceof Struct)) {
            return record;
        }

        Struct originalValue = (Struct) record.value();
        Schema originalSchema = record.valueSchema();

        Schema newSchema = rebuildSchema(originalSchema);
        Struct newValue = rebuildStruct(originalValue, originalSchema, newSchema);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema,
                newValue,
                record.timestamp()
        );
    }

    // ======================================================================
    // SCHEMA REBUILD — COPY FULL METADATA
    // ======================================================================
    private Schema rebuildSchema(Schema original) {
        if (original == null) return null;

        switch (original.type()) {

            case STRUCT:
                SchemaBuilder builder = SchemaBuilder.struct()
                        .name(original.name())
                        .doc(original.doc())
                        .version(original.version());

                if (original.isOptional()) builder.optional();
                if (original.defaultValue() != null) builder.defaultValue(original.defaultValue());
                if (original.parameters() != null) builder.parameters(original.parameters());

                for (Field f : original.fields()) {
                    Schema fieldSchema = f.schema();
                    Schema newFieldSchema;

                    if (convertAll && isZonedTimestamp(fieldSchema)) {
                        newFieldSchema = buildTimestampSchema(fieldSchema);
                    } else {
                        newFieldSchema = rebuildSchema(fieldSchema);
                    }

                    builder.field(f.name(), newFieldSchema);
                }

                return builder.build();

            case ARRAY:
                SchemaBuilder arrayBuilder =
                        SchemaBuilder.array(rebuildSchema(original.valueSchema()));
                if (original.isOptional()) arrayBuilder.optional();
                return arrayBuilder.build();

            case MAP:
                SchemaBuilder mapBuilder =
                        SchemaBuilder.map(
                                rebuildSchema(original.keySchema()),
                                rebuildSchema(original.valueSchema()));
                if (original.isOptional()) mapBuilder.optional();
                return mapBuilder.build();

            default:
                return original;
        }
    }

    private Schema buildTimestampSchema(Schema originalFieldSchema) {
        SchemaBuilder ts = Timestamp.builder()
                .doc(originalFieldSchema.doc())
                .version(originalFieldSchema.version());

        if (originalFieldSchema.isOptional()) {
            ts.optional();
        }

        if (originalFieldSchema.defaultValue() != null) {
            ts.defaultValue(originalFieldSchema.defaultValue());
        }

        if (originalFieldSchema.parameters() != null) {
            ts.parameters(originalFieldSchema.parameters());
        }

        return ts.build();
    }

    // ======================================================================
    // STRUCT REBUILD — APPLY VALUE CONVERSION
    // ======================================================================
    private Struct rebuildStruct(Struct originalValue, Schema originalSchema, Schema newSchema) {
        if (originalValue == null) return null;

        Struct result = new Struct(newSchema);

        for (Field f : originalSchema.fields()) {
            Object value = originalValue.get(f);
            Schema originalFieldSchema = f.schema();
            Schema newFieldSchema = newSchema.field(f.name()).schema();

            if (value == null) {
                result.put(f.name(), null);
                continue;
            }

            // ONLY PROCESS datetimeoffset fields
            if (convertAll && isZonedTimestamp(originalFieldSchema)) {
                result.put(f.name(), convertToTimestampDate(value));
                continue;
            }

            switch (originalFieldSchema.type()) {
                case STRUCT:
                    result.put(
                            f.name(),
                            rebuildStruct((Struct) value, originalFieldSchema, newFieldSchema)
                    );
                    break;

                case ARRAY:
                    List<?> list = (List<?>) value;
                    List<Object> newArr = new ArrayList<>(list.size());
                    newArr.addAll(list);
                    result.put(f.name(), newArr);
                    break;

                case MAP:
                    Map<?, ?> map = (Map<?, ?>) value;
                    Map<Object, Object> newMap = new HashMap<>();
                    newMap.putAll(map);
                    result.put(f.name(), newMap);
                    break;

                default:
                    result.put(f.name(), value);
            }
        }

        return result;
    }

    // ======================================================================
    // HELPERS
    // ======================================================================
    private boolean isZonedTimestamp(Schema schema) {
        return schema != null
                && schema.name() != null
                && schema.name().equals("io.debezium.time.ZonedTimestamp");
    }

    /**
     * Convert datetimeoffset → Kafka Timestamp nhưng GIỮ NGUYÊN GIÁ TRỊ GIỜ.
     *
     * Logic:
     * 1. Parse datetimeoffset thành OffsetDateTime
     * 2. Lấy LocalDateTime (bỏ offset đi)
     * 3. Interpret LocalDateTime này như UTC
     * 4. Convert thành epoch milli
     *
     * Kết quả:
     * - Timestamp trong Kafka hiển thị y HỆT giá trị trong DB
     * - Không bị trừ 7 tiếng nữa
     */
    private java.util.Date convertToTimestampDate(Object value) {
        if (value == null) return null;

        if (value instanceof java.util.Date) {
            return (java.util.Date) value;
        }

        if (value instanceof Instant) {
            return java.util.Date.from((Instant) value);
        }

        String txt = value.toString();

        try {
            // Parse datetimeoffset → có offset
            OffsetDateTime odt = OffsetDateTime.parse(txt);

            // Lấy local datetime (giờ phút giây đúng như DB nhập)
            LocalDateTime local = odt.toLocalDateTime();

            // Diễn giải local datetime này như UTC để ra epoch
            Instant reinterpretAsUTC = local.atZone(ZoneOffset.UTC).toInstant();

            return java.util.Date.from(reinterpretAsUTC);

        } catch (DateTimeParseException e) {
            throw new DataException("Failed to parse datetimeoffset: " + txt, e);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
}

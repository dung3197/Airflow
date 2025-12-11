package com.example.transforms;

// Các import cần thiết từ Kafka Connect và Java time API
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * --------------------------------------------------------------
 *  CLASS DateTimeOffsetToTimestamp
 * --------------------------------------------------------------
 *  Đây là một Kafka Connect SMT (Single Message Transform)
 *  được viết để sử dụng cho Debezium SQL Server Source Connector.
 *
 *  Nhiệm vụ:
 *  - Tìm các field có schema.name = "io.debezium.time.ZonedTimestamp"
 *    => đây là kiểu datetimeoffset của SQL Server được Debezium emit.
 *
 *  - Chuyển các field này sang logical Timestamp của Kafka Connect,
 *    nhưng *giữ nguyên giá trị giờ như trong DB* (KHÔNG shift timezone).
 *
 *  - Ứng xử này đảm bảo:
 *      + datetime (không timezone) được giữ nguyên → không đụng tới.
 *      + datetimeoffset cũng được giữ nguyên khi vào Kafka → không âm 7 tiếng.
 *
 * --------------------------------------------------------------
 */
public class DateTimeOffsetToTimestamp<R extends ConnectRecord<R>>
        implements Transformation<R> {

    /**
     * Tên config mà user có thể đặt trong connector config.
     * Ý nghĩa:
     *   - true: SMT sẽ convert tất cả các field datetimeoffset.
     *   - false: SMT không đụng vào bất kỳ field nào.
     */
    public static final String CONF_CONVERT_ALL = "convert.all.datetimeoffset";

    /**
     * Định nghĩa các config mà SMT hỗ trợ, gồm type, default, mô tả, v.v.
     * Kafka Connect dùng object ConfigDef để validate và show config info.
     */
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    CONF_CONVERT_ALL,                // tên config
                    ConfigDef.Type.BOOLEAN,         // kiểu dữ liệu
                    true,                           // default = true
                    ConfigDef.Importance.HIGH,      // mức độ quan trọng
                    "Convert all Debezium ZonedTimestamp fields (datetimeoffset) to Kafka Timestamp."
            );

    /**
     * Biến runtime chứa giá trị của config convert.all.datetimeoffset.
     * Được load trong phương thức configure().
     */
    private boolean convertAll;

    /**
     * Hàm configure() được Kafka Connect gọi khi SMT được khởi tạo.
     * Nó nhận vào Map config, dùng SimpleConfig để map sang kiểu đúng
     * và gán giá trị biến convertAll.
     */
    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.convertAll = config.getBoolean(CONF_CONVERT_ALL);
    }

    /**
     * --------------------------------------------------------------
     * apply() – Hàm chính của SMT.
     * --------------------------------------------------------------
     * Kafka Connect sẽ gọi hàm này cho từng record (key + value).
     *
     * Bước thực hiện:
     *  1. Kiểm tra record.value có phải Struct không (Debezium Envelope).
     *  2. Nếu không phải Struct → bỏ qua record.
     *  3. Nếu phải Struct → rebuild schema → rebuild struct → return record mới.
     */
    @Override
    public R apply(R record) {

        // Debezium record payload luôn là Struct.
        if (!(record.value() instanceof Struct)) {
            return record;   // nếu không phải Struct thì không transform
        }

        // Lấy Struct gốc (payload)
        Struct originalValue = (Struct) record.value();

        // Lấy schema gốc của payload
        Schema originalSchema = record.valueSchema();

        // Tạo schema mới phù hợp (đổi ZonedTimestamp → Timestamp)
        Schema newSchema = rebuildSchema(originalSchema);

        // Tạo Struct mới dựa theo schema mới (convert từng field nếu cần)
        Struct newValue = rebuildStruct(originalValue, originalSchema, newSchema);

        // Tạo record mới có value/schema mới, mọi thứ khác giữ nguyên
        return record.newRecord(
                record.topic(),         // giữ nguyên topic
                record.kafkaPartition(),// giữ nguyên partition
                record.keySchema(),     // giữ nguyên key schema
                record.key(),           // giữ nguyên key
                newSchema,              // schema mới
                newValue,               // value mới
                record.timestamp()      // giữ nguyên timestamp record
        );
    }

    // ======================================================================
    // SCHEMA REBUILD — COPY FULL METADATA
    // ======================================================================

    /**
     * rebuildSchema()
     *
     * Duyệt đệ quy toàn bộ schema.
     * - Nếu là STRUCT → tạo struct mới, duyệt từng field.
     * - Nếu field là ZonedTimestamp → build TS schema.
     * - Nếu không phải → xử lý đệ quy hoặc trả về schema gốc.
     *
     * Mục tiêu:
     * - Schema mới hoàn toàn giống schema gốc,
     *   chỉ thay đổi những field datetimeoffset.
     */
    private Schema rebuildSchema(Schema original) {
        if (original == null) return null;

        switch (original.type()) {

            // Case 1: STRUCT — xử lý lớn nhất
            case STRUCT:

                // SchemaBuilder struct mới – copy name/doc/version
                SchemaBuilder builder = SchemaBuilder.struct()
                        .name(original.name())
                        .doc(original.doc())
                        .version(original.version());

                // Preserve optional, default, parameters
                if (original.isOptional()) builder.optional();
                if (original.defaultValue() != null) builder.defaultValue(original.defaultValue());
                if (original.parameters() != null) builder.parameters(original.parameters());

                // Duyệt từng field trong struct gốc
                for (Field f : original.fields()) {
                    Schema fieldSchema = f.schema();
                    Schema newFieldSchema;

                    // Nếu là ZonedTimestamp → chuyển schema sang Timestamp
                    if (convertAll && isZonedTimestamp(fieldSchema)) {
                        newFieldSchema = buildTimestampSchema(fieldSchema);
                    } else {
                        // Ngược lại → xử lý đệ quy
                        newFieldSchema = rebuildSchema(fieldSchema);
                    }

                    builder.field(f.name(), newFieldSchema);
                }

                return builder.build();

            // Case 2: ARRAY
            case ARRAY:
                SchemaBuilder arrayBuilder =
                        SchemaBuilder.array(rebuildSchema(original.valueSchema()));
                if (original.isOptional()) arrayBuilder.optional();
                return arrayBuilder.build();

            // Case 3: MAP
            case MAP:
                SchemaBuilder mapBuilder =
                        SchemaBuilder.map(
                                rebuildSchema(original.keySchema()),
                                rebuildSchema(original.valueSchema()));
                if (original.isOptional()) mapBuilder.optional();
                return mapBuilder.build();

            // Case 4: PRIMITIVE hoặc logical khác → giữ nguyên
            default:
                return original;
        }
    }

    /**
     * buildTimestampSchema()
     *
     * Input: schema gốc type ZonedTimestamp
     * Output: schema logical Timestamp của Kafka Connect
     *
     * Đồng thời preserve:
     * - doc
     * - version
     * - optional
     * - defaultValue
     * - parameters
     */
    private Schema buildTimestampSchema(Schema originalFieldSchema) {
        SchemaBuilder ts = Timestamp.builder()          // Logical type "Timestamp"
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

    /**
     * rebuildStruct()
     *
     * Duyệt từng field:
     * - Nếu là ZonedTimestamp → convert value.
     * - Nếu STRUCT/ARRAY/MAP → đệ quy.
     * - Nếu primitive → copy nguyên giá trị.
     */
    private Struct rebuildStruct(Struct originalValue, Schema originalSchema, Schema newSchema) {
        if (originalValue == null) return null;

        Struct result = new Struct(newSchema);

        // Duyệt từng field trong struct gốc
        for (Field f : originalSchema.fields()) {
            Object value = originalValue.get(f);           // giá trị gốc
            Schema originalFieldSchema = f.schema();       // schema gốc field
            Schema newFieldSchema = newSchema.field(f.name()).schema(); // schema mới

            // Case null → ghi null luôn
            if (value == null) {
                result.put(f.name(), null);
                continue;
            }

            // Case cần convert datetimeoffset
            if (convertAll && isZonedTimestamp(originalFieldSchema)) {
                result.put(f.name(), convertToTimestampDate(value));
                continue;
            }

            // Không cần convert → xử lý theo type
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

    /**
     * Kiểm tra xem schema field có phải Debezium ZonedTimestamp hay không.
     * Đây là dấu hiệu duy nhất để nhận biết datetimeoffset.
     */
    private boolean isZonedTimestamp(Schema schema) {
        return schema != null
                && schema.name() != null
                && schema.name().equals("io.debezium.time.ZonedTimestamp");
    }

    /**
     * convertToTimestampDate()
     *
     * INPUT:
     *   value = "2025-08-08T07:39:21.253742+07:00"
     *
     * LOGIC:
     * 1. Parse chuỗi datetimeoffset → OffsetDateTime
     * 2. Lấy LocalDateTime (bỏ offset, giữ nguyên giờ/phút/giây như DB)
     * 3. Interpret local time này như UTC → tạo Instant với giờ giữ nguyên
     * 4. Convert Instant → java.util.Date
     *
     * OUTPUT:
     *   Timestamp trong Kafka = GIỮ NGUYÊN GIÁ TRỊ GIỜ NHƯ DB.
     */
    private java.util.Date convertToTimestampDate(Object value) {
        if (value == null) return null;

        // Nếu giá trị đã là java.util.Date → return ngay
        if (value instanceof java.util.Date) {
            return (java.util.Date) value;
        }

        // Nếu giá trị đã là Instant → return luôn (không shift)
        if (value instanceof Instant) {
            return java.util.Date.from((Instant) value);
        }

        String txt = value.toString();

        try {
            // Parse datetimeoffset → ví dụ: "2025-08-08T07:39:21.253742+07:00"
            OffsetDateTime odt = OffsetDateTime.parse(txt);

            // Lấy LocalDateTime (giờ DB nhập, không thay đổi)
            LocalDateTime local = odt.toLocalDateTime();

            // Diễn giải local time như UTC để tính epoch milli
            Instant reinterpretAsUTC = local.atZone(ZoneOffset.UTC).toInstant();

            // Trả về java.util.Date vì Kafka Timestamp yêu cầu type này
            return java.util.Date.from(reinterpretAsUTC);

        } catch (DateTimeParseException e) {
            throw new DataException(
                    "Failed to parse datetimeoffset: " + txt, e
            );
        }
    }

    // ======================================================================
    // Required method của Transformation interface
    // ======================================================================
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // Không cần cleanup resource trong SMT này
    }
}

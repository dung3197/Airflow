# SOP VẬN HÀNH TỰ ĐỘNG CAPTURE INSTANCE CHO DEBEZIUM SQL SERVER CDC

**Phiên bản:** 1.0  
**Phạm vi:** SQL Server → Debezium SQL Server Source Connector → Kafka → S3 Sink Connector → MinIO  
**Mục tiêu:** Tự động xử lý thay đổi schema của các bảng đang CDC mà không để DML mới bị capture bằng schema cũ.

---

# 1. Mục tiêu

Quy trình này giải quyết tình huống:

```sql
ALTER TABLE dbo.customer
ADD phone_number VARCHAR(20);
```

sau đó application thực hiện:

```sql
INSERT INTO dbo.customer (..., phone_number)
VALUES (..., '0901234567');
```

hoặc:

```sql
UPDATE dbo.customer
SET phone_number = '0901234567'
WHERE customer_id = 1001;
```

Khi SQL Server CDC đã được enable trước đó, capture table hiện tại **không tự động bổ sung column mới**. Debezium yêu cầu phải tạo capture instance mới để chuyển sang schema mới. Online schema update thông thường có một khoảng thời gian giữa `ALTER TABLE` và việc tạo capture instance mới; DML trong khoảng này vẫn đi vào change table cũ và không chứa column mới.

Quy trình này phải bảo đảm:

1. Tự phát hiện `ALTER TABLE` trên bảng đang CDC.
2. Không cần DBA thao tác thủ công tạo capture instance.
3. Không cho INSERT/UPDATE/DELETE chạy trong khoảng schema chưa an toàn.
4. Tự tạo capture instance mới.
5. Tự kiểm tra capture instance mới.
6. Cho application tiếp tục ghi dữ liệu sau khi kiểm tra thành công.
7. Giữ capture instance cũ cho Debezium đọc hết.
8. Chỉ xóa instance cũ sau khi Debezium báo hoàn thành.
9. Có audit log đầy đủ.
10. Không để tồn tại capture instance rác.

---

# 2. Nguyên tắc quan trọng

## 2.1. Không gọi `sp_cdc_enable_table` trực tiếp trong DDL trigger

DDL trigger chỉ thực hiện:

```text
Detect
  ↓
Audit
  ↓
Set trạng thái PENDING
  ↓
Đưa request vào queue
```

Không thực hiện:

```text
DDL Trigger
   ↓
sp_cdc_enable_table
```

Việc tạo capture instance được thực hiện bởi một worker riêng sau khi transaction DDL đã COMMIT.

Trigger cần chạy càng ngắn càng tốt vì trigger nằm trong transaction của statement gây ra trigger. Microsoft cũng khuyến nghị giảm thời gian xử lý trong trigger để tránh giữ lock lâu.

---

## 2.2. Fail-closed

Nếu automation gặp lỗi:

```text
ALTER TABLE
     ↓
automation ERROR
     ↓
DML BLOCKED
```

Không được:

```text
ALTER TABLE
     ↓
automation ERROR
     ↓
DML vẫn chạy
     ↓
mất column mới trong CDC
```

An toàn dữ liệu được ưu tiên hơn availability trong khoảng schema migration.

---

# 3. Kiến trúc

```text
                       SQL SERVER
                           |
                     ALTER TABLE
                           |
                           v
                +---------------------+
                | Database DDL Trigger|
                +----------+----------+
                           |
             +-------------+-------------+
             |                           |
             v                           v
      cdc_admin audit             table_state
      rotation_queue              = PENDING
                                         |
                                         |
Application DML                           |
      |                                  |
      v                                  |
DML Guard Trigger                        |
      |                                  |
      +---- state=PENDING? ---- YES -----+
      |                   |
      |                  BLOCK
      |
      | READY/DRAINING
      v
    ALLOW


rotation_queue
      |
      v
CDC Schema Manager
(Python service / Worker)
      |
      +--> create new capture instance
      |
      +--> validate instance
      |
      +--> validate columns
      |
      +--> state = DRAINING
                        |
                        v
                    Debezium
                        |
                        v
                      Kafka
                        |
                        v
                     S3 Sink
                        |
                        v
                      MinIO


Debezium notification topic
           |
           v
 Capture Instance COMPLETED
           |
           v
 CDC Schema Manager
           |
           v
sp_cdc_disable_table(old)
           |
           v
       state=READY
```

---

# 4. Naming convention

Chuẩn khuyến nghị:

```text
<schema>_<table>_<ddMMyyyyHHmmss>
```

Ví dụ:

```text
dbo_customer_27082026163645
```

tương ứng:

```text
27/08/2026 16:36:45
```

Không nên chỉ sử dụng:

```text
dbo_customer_27082026
```

vì một bảng có thể thay đổi schema nhiều lần trong cùng một ngày.

SQL Server yêu cầu `capture_instance` phải unique trong database, tối đa 100 ký tự và một source table chỉ được có tối đa **hai capture instances** cùng lúc.

Nếu naming convention bắt buộc của hệ thống là:

```text
<schema><table>_<ddMMyyyyHHmmss>
```

ví dụ:

```text
dbocustomer_27082026163645
```

thì chỉ cần thay hàm generate name; toàn bộ quy trình không thay đổi.

---

# 5. Trạng thái vận hành

Sử dụng các trạng thái:

| State | Ý nghĩa | DML |
|---|---|---|
| `READY` | Chỉ có capture hiện hành | Cho phép |
| `PENDING` | Phát hiện ALTER | Chặn |
| `CREATING` | Đang tạo capture mới | Chặn |
| `VALIDATING` | Đang kiểm tra capture mới | Chặn |
| `DRAINING` | Capture mới OK, Debezium đang đọc instance cũ | Cho phép |
| `CLEANUP_PENDING` | Chờ xóa instance cũ | Cho phép |
| `ERROR` | Automation gặp lỗi trước khi schema an toàn | Chặn |

Luồng chuẩn:

```text
READY
  ↓
ALTER TABLE
  ↓
PENDING
  ↓
CREATING
  ↓
VALIDATING
  ↓
DRAINING
  ↓
Debezium COMPLETED
  ↓
CLEANUP_PENDING
  ↓
READY
```

---

# 6. Thứ tự triển khai lần đầu, database và quyền

Phần này là thứ tự bắt buộc khi triển khai giải pháp từ đầu. Không tạo DDL trigger trước khi metadata, DML Guard, worker và Debezium notification đã sẵn sàng.

Trong toàn bộ tài liệu, sử dụng quy ước:

```text
[SourceDB] = database SQL Server đang được Debezium capture.

Ví dụ:
[SourceDB] = CustomerDB

Table ví dụ:
CustomerDB.dbo.customer
```

## 6.1. Thành phần nào nằm ở đâu

| Thành phần | Nơi tạo/chạy | Ghi chú |
|---|---|---|
| `cdc_admin` schema | `[SourceDB]` | Cùng database với bảng nguồn |
| `cdc_admin.table_state` | `[SourceDB]` | Không đặt ở `master` hoặc `msdb` |
| `cdc_admin.rotation_queue` | `[SourceDB]` | Cùng transaction với DDL trigger |
| `cdc_admin.rotation_log` | `[SourceDB]` | Audit của automation |
| Database DDL Trigger | `[SourceDB]` | `ON DATABASE` nên chỉ bắt DDL của database này |
| DML Guard Trigger | `[SourceDB]`, trên từng source table | Ví dụ `dbo.customer` |
| CDC capture instance | `[SourceDB]`, schema hệ thống `cdc` | Do `sys.sp_cdc_enable_table` tạo |
| CDC Rotation Worker | Chạy ngoài SQL Server hoặc SQL Agent | Kết nối vào `[SourceDB]` |
| Worker login | SQL Server instance | Login là server-level principal; user tương ứng nằm trong `[SourceDB]` |
| SQL Agent job | `msdb` | Chỉ cần nếu chọn SQL Agent thay cho worker ngoài |
| Debezium notification config | Kafka Connect | Không chạy trong SQL Server |
| `debezium-sqlserver-notifications` | Kafka | Không tạo trong SQL Server |
| S3 Sink Connector | Kafka Connect | Không thay đổi cho cơ chế rotation |
| MinIO | MinIO | Không cần object quản trị CDC nào |

Không đặt `table_state`, `rotation_queue` hoặc `rotation_log` trong `master`/`msdb`. Đặt chúng trong chính `[SourceDB]` giúp DDL trigger ghi trạng thái trong cùng database và tránh phụ thuộc cross-database permission.

## 6.2. Quyền cần thiết

### Account triển khai/DBA

Khuyến nghị dùng một DBA account trong thời gian cài đặt. Quyền tối thiểu phụ thuộc lệnh:

| Lệnh | Scope | Quyền tối thiểu |
|---|---|---|
| `sys.sp_cdc_enable_db` | `[SourceDB]` | Với SQL Server: `sysadmin` server role |
| `CREATE SCHEMA` | `[SourceDB]` | `CREATE SCHEMA`; nếu đặt owner là `dbo`, account phải có quyền phù hợp để gán owner; DBA/db_owner là lựa chọn triển khai đơn giản |
| `CREATE TABLE` trong `cdc_admin` | `[SourceDB]` | `CREATE TABLE` trong database + `ALTER` trên schema `cdc_admin` |
| Tạo DDL trigger `ON DATABASE` | `[SourceDB]` | `ALTER ANY DATABASE DDL TRIGGER` |
| Tạo DML trigger trên bảng | `[SourceDB]` | `ALTER` trên từng table |
| `sys.sp_cdc_enable_table` | `[SourceDB]` | membership `db_owner` |
| `sys.sp_cdc_disable_table` | `[SourceDB]` | membership `db_owner` |
| `sys.sp_cdc_help_change_data_capture` | `[SourceDB]` | `SELECT` trên captured columns + gating role nếu có; `db_owner` thấy toàn bộ |

### Account chạy Worker

Baseline của SOP này dùng account riêng, ví dụ:

```text
svc_cdc_rotation
```

Worker phải gọi được `sp_cdc_enable_table` và `sp_cdc_disable_table`. Theo quyền chính thức của hai stored procedure này, baseline đơn giản và rõ ràng nhất là:

```text
[SourceDB]: db_owner
Server: KHÔNG cần sysadmin
```

Không dùng account của application và không dùng account Debezium cho worker.

### Account Application

Application chỉ cần quyền nghiệp vụ bình thường trên source table, ví dụ:

```text
SELECT / INSERT / UPDATE / DELETE
```

Application không cần `SELECT`, `INSERT` hoặc `UPDATE` trực tiếp trên `cdc_admin.*` vì các trigger trong SOP này chạy bằng:

```sql
WITH EXECUTE AS OWNER
```

### Account Developer chạy ALTER

Developer/migration account cần quyền DDL theo chính sách của doanh nghiệp, ví dụ `ALTER` trên table cần thay đổi. Developer không cần quyền ghi trực tiếp vào `cdc_admin.*`; DDL trigger chạy bằng `EXECUTE AS OWNER`.

### Account Debezium

Debezium không được dùng để tạo/xóa capture instance. Account Debezium chỉ cần quyền đọc cần thiết đối với source/CDC theo cấu hình CDC, bao gồm `SELECT` trên các captured columns và membership trong gating role nếu capture instance có `@role_name`.

## 6.3. Thứ tự triển khai bắt buộc

Thứ tự production nên là:

```text
Bước 0  Kiểm tra CDC database/source table hiện tại
   ↓
Bước 1  Tạo cdc_admin schema
   ↓
Bước 2  Tạo table_state
   ↓
Bước 3  Tạo rotation_queue
   ↓
Bước 4  Tạo rotation_log
   ↓
Bước 5  Seed table_state cho toàn bộ bảng CDC
   ↓
Bước 6  Tạo account Worker và cấp db_owner trên SourceDB
   ↓
Bước 7  Deploy/start Worker
   ↓
Bước 8  Cấu hình Debezium notification và kiểm tra Kafka topic
   ↓
Bước 9  Tạo DML Guard cho từng bảng CDC
   ↓
Bước 10 Tạo Database DDL Trigger
   ↓
Bước 11 Chạy smoke test
   ↓
Bước 12 Cho phép schema deployment bình thường
```

Trong thời gian Bước 1 đến Bước 11 nên áp dụng change freeze đối với `ALTER TABLE` trên các bảng CDC.

Không đổi thứ tự thành:

```text
DDL Trigger → metadata → worker
```

vì trigger có thể fire khi dependency chưa tồn tại.

## 6.4. Bước 0 — kiểm tra CDC database

Chạy tại SQL Server instance:

```sql
SELECT
    name,
    is_cdc_enabled
FROM sys.databases
WHERE name = N'SourceDB';
```

Expected:

```text
SourceDB    1
```

Nếu `is_cdc_enabled = 0` và đây là lần đầu enable CDC cho database:

```sql
USE [SourceDB];
GO

EXEC sys.sp_cdc_enable_db;
GO
```

Lệnh này chỉ chạy một lần cho database. Trên SQL Server, account chạy lệnh phải là `sysadmin`.

Nếu hệ thống Debezium hiện tại đã chạy CDC bình thường và `is_cdc_enabled = 1`, không chạy lại bước enable database.

Kiểm tra các table đang CDC:

```sql
USE [SourceDB];
GO

SELECT
    s.name AS schema_name,
    t.name AS table_name,
    t.is_tracked_by_cdc
FROM sys.tables t
JOIN sys.schemas s
    ON s.schema_id = t.schema_id
WHERE t.is_tracked_by_cdc = 1
ORDER BY s.name, t.name;
```

## 6.5. Bước 1 — tạo schema quản trị

**Database:** `[SourceDB]`  
**Chạy:** một lần khi cài đặt  
**Account:** DBA/deployment account

```sql
USE [SourceDB];
GO

CREATE SCHEMA cdc_admin AUTHORIZATION dbo;
GO
```

## 6.6. Bước 2 — tạo bảng trạng thái

**Database:** `[SourceDB]`  
**Chạy:** một lần

```sql
USE [SourceDB];
GO

CREATE TABLE cdc_admin.table_state
(
    source_object_id INT NOT NULL PRIMARY KEY,
    schema_name SYSNAME NOT NULL,
    table_name SYSNAME NOT NULL,
    state VARCHAR(30) NOT NULL,
    old_capture_instance SYSNAME NULL,
    new_capture_instance SYSNAME NULL,
    correlation_id UNIQUEIDENTIFIER NULL,
    last_error NVARCHAR(4000) NULL,
    detected_at DATETIME2(3) NULL,
    updated_at DATETIME2(3) NOT NULL
        CONSTRAINT DF_cdc_table_state_updated
        DEFAULT SYSUTCDATETIME()
);
GO
```

## 6.7. Bước 3 — tạo Queue

**Database:** `[SourceDB]`  
**Chạy:** một lần

```sql
USE [SourceDB];
GO

CREATE TABLE cdc_admin.rotation_queue
(
    request_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    correlation_id UNIQUEIDENTIFIER NOT NULL
        CONSTRAINT UQ_rotation_queue_correlation UNIQUE,
    source_object_id INT NOT NULL,
    schema_name SYSNAME NOT NULL,
    table_name SYSNAME NOT NULL,
    ddl_command NVARCHAR(MAX) NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    created_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    started_at DATETIME2(3) NULL,
    finished_at DATETIME2(3) NULL,
    last_error NVARCHAR(4000) NULL
);
GO
```

## 6.8. Bước 4 — tạo Audit log

**Database:** `[SourceDB]`  
**Chạy:** một lần

```sql
USE [SourceDB];
GO

CREATE TABLE cdc_admin.rotation_log
(
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    correlation_id UNIQUEIDENTIFIER NOT NULL,
    source_object_id INT NULL,
    schema_name SYSNAME NULL,
    table_name SYSNAME NULL,
    event_name VARCHAR(50) NOT NULL,
    old_capture_instance SYSNAME NULL,
    new_capture_instance SYSNAME NULL,
    ddl_command NVARCHAR(MAX) NULL,
    login_name SYSNAME NULL,
    host_name NVARCHAR(128) NULL,
    application_name NVARCHAR(128) NULL,
    error_number INT NULL,
    error_message NVARCHAR(4000) NULL,
    created_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
```

Ví dụ `event_name`:

```text
DDL_DETECTED
ROTATION_STARTED
CAPTURE_CREATED
CAPTURE_VALIDATED
DML_RELEASED
DEBEZIUM_COMPLETED
OLD_CAPTURE_DISABLED
ROTATION_COMPLETED
ROTATION_ERROR
```

---

# 7. Bước 5 — đăng ký trạng thái ban đầu

**Database:** `[SourceDB]`  
**Chạy:** một lần trong lần cài đặt đầu tiên; sau đó worker duy trì trạng thái  
**Account:** DBA/deployment account

Trước khi seed, bắt buộc kiểm tra mỗi source table chỉ có một capture instance. Nếu đã có hai instance từ một migration cũ, dừng triển khai và xử lý dứt điểm trước.

```sql
USE [SourceDB];
GO

SELECT
    source_object_id,
    COUNT(*) AS capture_count
FROM cdc.change_tables
GROUP BY source_object_id
HAVING COUNT(*) > 1;
```

Expected trước khi go-live automation:

```text
0 rows
```

Xem cấu hình CDC:

```sql
USE [SourceDB];
GO

EXEC sys.sp_cdc_help_change_data_capture;
GO
```

Seed toàn bộ table đang có đúng một capture instance:

```sql
USE [SourceDB];
GO

INSERT INTO cdc_admin.table_state
(
    source_object_id,
    schema_name,
    table_name,
    state,
    old_capture_instance,
    updated_at
)
SELECT
    ct.source_object_id,
    s.name,
    t.name,
    'READY',
    ct.capture_instance,
    SYSUTCDATETIME()
FROM cdc.change_tables ct
JOIN sys.tables t
    ON t.object_id = ct.source_object_id
JOIN sys.schemas s
    ON s.schema_id = t.schema_id
WHERE NOT EXISTS
(
    SELECT 1
    FROM cdc_admin.table_state x
    WHERE x.source_object_id = ct.source_object_id
);
GO
```

Kiểm tra:

```sql
SELECT *
FROM cdc_admin.table_state
ORDER BY schema_name, table_name;
```

Ở trạng thái `READY`, cột `old_capture_instance` được hiểu là capture instance hiện hành. Tên cột này được giữ để phục vụ logic rotation; sau khi cleanup, instance mới được promote vào trường này.

---

# 8. Bước 10 — Database DDL Trigger

Trigger này phát hiện:

```sql
ALTER TABLE
```

trên bảng đang CDC.

DDL trigger chỉ được tạo/enable sau khi hoàn tất metadata, seed state, worker, Debezium notification và DML Guard.

**Database:** `[SourceDB]`  
**Chạy:** một lần khi cài đặt; sau đó tự fire khi có `ALTER TABLE`  
**Quyền tạo:** `ALTER ANY DATABASE DDL TRIGGER` trong `[SourceDB]`

```sql
USE [SourceDB];
GO

CREATE OR ALTER TRIGGER trg_cdc_schema_change
ON DATABASE
WITH EXECUTE AS OWNER
FOR ALTER_TABLE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @event XML = EVENTDATA();

    DECLARE @schema_name SYSNAME =
        @event.value(
            '(/EVENT_INSTANCE/SchemaName)[1]',
            'SYSNAME'
        );

    DECLARE @table_name SYSNAME =
        @event.value(
            '(/EVENT_INSTANCE/ObjectName)[1]',
            'SYSNAME'
        );

    DECLARE @ddl NVARCHAR(MAX) =
        @event.value(
            '(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]',
            'NVARCHAR(MAX)'
        );

    DECLARE @object_id INT =
        OBJECT_ID(
            QUOTENAME(@schema_name)
            + '.'
            + QUOTENAME(@table_name)
        );

    ---------------------------------------------------
    -- Không phải table
    ---------------------------------------------------

    IF @object_id IS NULL
        RETURN;

    ---------------------------------------------------
    -- Không CDC
    ---------------------------------------------------

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.tables
        WHERE object_id = @object_id
          AND is_tracked_by_cdc = 1
    )
        RETURN;

    ---------------------------------------------------
    -- Không cho ALTER tiếp khi migration trước chưa xong
    ---------------------------------------------------

    IF
    (
        SELECT COUNT(*)
        FROM cdc.change_tables
        WHERE source_object_id = @object_id
    ) >= 2
    BEGIN
        RAISERROR(
            'CDC schema migration chưa hoàn tất: table hiện đang có 2 capture instances.',
            16,
            1
        );

        ROLLBACK TRANSACTION;
        RETURN;
    END;

    IF EXISTS
    (
        SELECT 1
        FROM cdc_admin.table_state
        WHERE source_object_id = @object_id
          AND state <> 'READY'
    )
    BEGIN
        RAISERROR(
            'CDC schema migration đang được xử lý cho table này.',
            16,
            1
        );

        ROLLBACK TRANSACTION;
        RETURN;
    END;

    DECLARE @correlation_id UNIQUEIDENTIFIER = NEWID();

    DECLARE @old_capture SYSNAME;

    SELECT TOP (1)
        @old_capture = capture_instance
    FROM cdc.change_tables
    WHERE source_object_id = @object_id
    ORDER BY create_date DESC;

    ---------------------------------------------------
    -- PENDING
    ---------------------------------------------------

    UPDATE cdc_admin.table_state
    SET
        state = 'PENDING',
        old_capture_instance = @old_capture,
        new_capture_instance = NULL,
        correlation_id = @correlation_id,
        detected_at = SYSUTCDATETIME(),
        updated_at = SYSUTCDATETIME(),
        last_error = NULL
    WHERE source_object_id = @object_id;

    ---------------------------------------------------
    -- Queue
    ---------------------------------------------------

    INSERT INTO cdc_admin.rotation_queue
    (
        correlation_id,
        source_object_id,
        schema_name,
        table_name,
        ddl_command,
        status
    )
    VALUES
    (
        @correlation_id,
        @object_id,
        @schema_name,
        @table_name,
        @ddl,
        'PENDING'
    );

    ---------------------------------------------------
    -- Audit
    ---------------------------------------------------

    INSERT INTO cdc_admin.rotation_log
    (
        correlation_id,
        source_object_id,
        schema_name,
        table_name,
        event_name,
        old_capture_instance,
        ddl_command,
        login_name,
        host_name,
        application_name
    )
    VALUES
    (
        @correlation_id,
        @object_id,
        @schema_name,
        @table_name,
        'DDL_DETECTED',
        @old_capture,
        @ddl,
        ORIGINAL_LOGIN(),
        HOST_NAME(),
        APP_NAME()
    );
END;
GO
```

`EVENTDATA()` lấy thông tin DDL event. `WITH EXECUTE AS OWNER` là bắt buộc trong baseline này để developer chạy `ALTER TABLE` không cần được cấp quyền trực tiếp trên `cdc_admin.table_state`, `rotation_queue` và `rotation_log`.

Sau khi tạo, kiểm tra trigger đang enabled:

```sql
SELECT
    name,
    is_disabled
FROM sys.triggers
WHERE parent_class_desc = 'DATABASE'
  AND name = N'trg_cdc_schema_change';
```

Expected:

```text
is_disabled = 0
```

---

# 9. Bước 9 — DML Guard

Đây là thành phần quan trọng nhất để tránh CDC gap.

Ví dụ bảng:

```text
dbo.customer
```

Tạo:

**Database:** `[SourceDB]`  
**Tạo trước DDL trigger**  
**Quyền tạo:** `ALTER` trên source table

```sql
USE [SourceDB];
GO

CREATE OR ALTER TRIGGER dbo.trg_cdc_guard_customer
ON dbo.customer
WITH EXECUTE AS OWNER
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF ROWCOUNT_BIG() = 0
        RETURN;

    IF EXISTS
    (
        SELECT 1
        FROM cdc_admin.table_state
        WHERE source_object_id = OBJECT_ID(N'dbo.customer')
          AND state IN
          (
              'PENDING',
              'CREATING',
              'VALIDATING',
              'ERROR'
          )
    )
    BEGIN
        RAISERROR(
            'CDC_SCHEMA_ROTATION_IN_PROGRESS: DML tạm thời bị khóa.',
            16,
            1
        );

        ROLLBACK TRANSACTION;
        RETURN;
    END;
END;
GO
```

`WITH EXECUTE AS OWNER` giúp application không cần `SELECT` trên `cdc_admin.table_state`. Application chỉ giữ quyền DML nghiệp vụ trên `dbo.customer`.

SQL Server xác nhận DML trigger và statement gây trigger nằm trong cùng transaction; `ROLLBACK TRANSACTION` trong trigger rollback các thay đổi của transaction và dừng phần còn lại của batch.

Application nên coi lỗi:

```text
CDC_SCHEMA_ROTATION_IN_PROGRESS
```

là lỗi có thể retry.

Ví dụ:

```text
retry 1 → 500 ms
retry 2 → 1 s
retry 3 → 2 s
retry 4 → 4 s
```

---

# 10. Bước 6 và 7 — tạo account, deploy Worker

Khuyến nghị chạy:

```text
cdc-schema-manager
```

dưới dạng Python service hoặc service nội bộ tương đương.

## 10.1. Worker chạy ở đâu

Worker không phải object bắt buộc nằm trong SQL Server. Khuyến nghị chạy trên application/utility host hoặc Kubernetes và kết nối TCP tới SQL Server `[SourceDB]`.

Nếu dùng Worker ngoài SQL Server thì không cần tạo job trong `msdb`.

## 10.2. Tạo login/user cho Worker

Ví dụ dùng Windows/AD service account được ưu tiên. Nếu minh họa bằng SQL Login:

```sql
USE [master];
GO

CREATE LOGIN [svc_cdc_rotation]
WITH PASSWORD = 'Use_A_Secret_From_Vault_Not_Hardcoded';
GO
```

`CREATE LOGIN` là thao tác server-level. Account triển khai cần quyền tạo login phù hợp, ví dụ `ALTER ANY LOGIN` hoặc quyền quản trị tương đương.

Sau đó map login vào source database:

```sql
USE [SourceDB];
GO

CREATE USER [svc_cdc_rotation]
FOR LOGIN [svc_cdc_rotation];
GO

ALTER ROLE db_owner
ADD MEMBER [svc_cdc_rotation];
GO
```

Lý do `db_owner`: worker cần gọi cả `sys.sp_cdc_enable_table` và `sys.sp_cdc_disable_table`, và hai procedure này yêu cầu `db_owner` trong current database.

Worker **không cần** `sysadmin` server role.

Trong production không hard-code password như ví dụ trên; sử dụng Windows authentication, Microsoft Entra/AD phù hợp môi trường, Vault hoặc secret manager.

Luồng:

```text
SELECT PENDING
   ↓
claim request
   ↓
CREATING
   ↓
generate capture name
   ↓
sp_cdc_enable_table
   ↓
VALIDATING
   ↓
validation
   ↓
DRAINING
```

SQL Agent cũng có thể sử dụng, nhưng schedule theo giây của SQL Server Agent phải có interval tối thiểu 10 giây. Nếu sử dụng SQL Agent thuần túy, DML có thể bị chặn tới khoảng thời gian này; một worker chạy liên tục sẽ cho thời gian phản hồi tốt hơn.

---

# 11. Generate capture instance name

Ví dụ T-SQL:

```sql
DECLARE @schema_name SYSNAME = N'dbo';
DECLARE @table_name SYSNAME = N'customer';

DECLARE @now DATETIME2(0) = SYSDATETIME();

DECLARE @timestamp VARCHAR(14) =
      RIGHT('0' + CONVERT(VARCHAR(2), DAY(@now)), 2)
    + RIGHT('0' + CONVERT(VARCHAR(2), MONTH(@now)), 2)
    + CONVERT(VARCHAR(4), YEAR(@now))
    + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(HOUR, @now)), 2)
    + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(MINUTE, @now)), 2)
    + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(SECOND, @now)), 2);

DECLARE @capture_instance SYSNAME =
    CONCAT(
        @schema_name,
        '_',
        @table_name,
        '_',
        @timestamp
    );

IF LEN(@capture_instance) > 100
BEGIN
    THROW 51020,
        'CDC capture instance name exceeds 100 characters.',
        1;
END;

SELECT @capture_instance;
```

Ví dụ:

```text
dbo_customer_27082026163645
```

---

# 12. Lấy cấu hình capture instance cũ

Trước khi tạo instance mới, worker phải lưu:

```text
supports_net_changes
role_name
index_name
filegroup_name
captured_column_list
partition_switch
```

Không nên hard-code vì có thể làm capture instance mới khác policy instance cũ.

Kiểm tra:

```sql
EXEC sys.sp_cdc_help_change_data_capture
    @source_schema = N'dbo',
    @source_name = N'customer';
```

Microsoft khuyến nghị sử dụng procedure này để xem configuration CDC thay vì phụ thuộc vào truy vấn trực tiếp system tables.

---

# 13. Chính sách column

Có hai lựa chọn.

## Option A — capture tất cả columns

```sql
@captured_column_list = NULL
```

Phù hợp khi tất cả column của bảng đều được phép đưa tới Kafka/MinIO.

---

## Option B — explicit allowlist

Ví dụ:

```text
customer_id
customer_name
phone_number
updated_at
```

Không capture:

```text
password_hash
secret_token
private_note
```

Đối với dữ liệu nhạy cảm, khuyến nghị Option B.

Theo Microsoft, CDC mặc định capture toàn bộ source columns nếu không cung cấp danh sách riêng.

---

# 14. Worker tạo capture instance mới

Ví dụ:

**Database thực thi:** luôn là `[SourceDB]`, vì `sp_cdc_enable_table` thao tác trên current database.

```sql
USE [SourceDB];
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'customer',

    @role_name = NULL,

    @capture_instance =
        N'dbo_customer_27082026163645',

    @supports_net_changes = 0;
GO
```

Với production cần truyền lại các cấu hình tương ứng từ capture instance cũ.

`sys.sp_cdc_enable_table` yêu cầu membership `db_owner` trong `[SourceDB]`. Chỉ worker account thực hiện lệnh này trong vận hành bình thường; developer và application không thực hiện trực tiếp.

---

# 15. Validate capture instance mới

Không mở lại DML ngay sau khi:

```sql
EXEC sys.sp_cdc_enable_table ...
```

Worker chuyển:

```text
CREATING
   ↓
VALIDATING
```

và thực hiện các bước sau.

## 15.1. Kiểm tra có đúng 2 capture instances

```sql
EXEC sys.sp_cdc_help_change_data_capture
    @source_schema = N'dbo',
    @source_name = N'customer';
```

Expected:

```text
dbo_customer_26082026100000
dbo_customer_27082026163645
```

---

## 15.2. Kiểm tra instance mới tồn tại

```sql
SELECT
    capture_instance,
    start_lsn,
    create_date
FROM cdc.change_tables
WHERE source_object_id =
      OBJECT_ID(N'dbo.customer');
```

Expected:

```text
capture_instance                   start_lsn
---------------------------------  ----------------
dbo_customer_26082026100000        NOT NULL
dbo_customer_27082026163645        NOT NULL
```

---

## 15.3. Kiểm tra cột mới

Ví dụ:

```sql
ALTER TABLE dbo.customer
ADD phone_number VARCHAR(20);
```

Kiểm tra:

```sql
SELECT
    ct.capture_instance,
    cc.column_name,
    cc.column_type
FROM cdc.change_tables ct
JOIN cdc.captured_columns cc
    ON cc.object_id = ct.object_id
WHERE ct.capture_instance =
      N'dbo_customer_27082026163645'
ORDER BY cc.column_ordinal;
```

Expected:

```text
customer_id
customer_name
phone_number       <-- phải tồn tại
updated_at
```

---

# 16. Release DML

Nếu validation thành công:

```sql
UPDATE cdc_admin.table_state
SET
    state = 'DRAINING',
    new_capture_instance =
        N'dbo_customer_27082026163645',
    updated_at = SYSUTCDATETIME(),
    last_error = NULL
WHERE source_object_id =
      OBJECT_ID(N'dbo.customer');
```

Từ thời điểm này:

```text
DML Guard
   ↓
state = DRAINING
   ↓
ALLOW
```

Application bắt đầu INSERT/UPDATE bình thường.

---

# 17. Bước 8 — Debezium configuration

Đây **không phải lệnh SQL** và không chạy trong `[SourceDB]`.

Thực hiện trên Kafka Connect configuration của chính Debezium SQL Server Source Connector trước khi bật DDL trigger.

Enable notifications:

```json
{
  "notification.enabled.channels": "sink,log",
  "notification.sink.topic.name":
      "debezium-sqlserver-notifications"
}
```

Debezium hỗ trợ `sink`, `log` và `jmx`. Khi sử dụng `sink`, phải cấu hình `notification.sink.topic.name`.

Sau khi cập nhật connector, phải kiểm tra connector/task ở trạng thái RUNNING và topic notification có thể được worker consume trước khi chuyển sang Bước 9/10.

---

# 18. Debezium chuyển capture instance

Trong giai đoạn:

```text
DRAINING
```

SQL Server có:

```text
OLD capture instance
+
NEW capture instance
```

Đây là trạng thái hợp lệ.

Không được xóa instance cũ ngay.

Debezium sẽ đọc dữ liệu cũ còn tồn đọng và chuyển sang capture instance mới. Tài liệu Debezium yêu cầu giữ instance cũ cho tới khi connector hoàn tất xử lý nó.

---

# 19. Debezium COMPLETED notification

Ví dụ message:

```json
{
  "aggregate_type": "Capture Instance",
  "type": "COMPLETED",
  "additional_data": {
    "connector_name": "sqlserver-source",
    "capture_instance":
        "dbo_customer_26082026100000",
    "database": "customer_db",
    "start_lsn": "...",
    "stop_lsn": "...",
    "commit_lsn": "..."
  }
}
```

Debezium SQL Server connector có notification `Capture Instance / COMPLETED` phục vụ chính quá trình schema evolution này.

Worker chỉ cleanup khi:

```text
aggregate_type = Capture Instance
AND
type = COMPLETED
AND
capture_instance = old_capture_instance
```

---

# 20. Xóa capture instance cũ

Ví dụ:

**Ai chạy:** CDC Rotation Worker  
**Database:** `[SourceDB]`  
**Điều kiện bắt buộc:** Debezium đã phát `Capture Instance / COMPLETED` đúng với `old_capture_instance`

```sql
USE [SourceDB];
GO

EXEC sys.sp_cdc_disable_table
    @source_schema = N'dbo',
    @source_name = N'customer',
    @capture_instance =
        N'dbo_customer_26082026100000';
GO
```

`sp_cdc_disable_table` xóa change table và các system functions liên quan tới capture instance đó, vì vậy chỉ được thực hiện sau khi Debezium đã hoàn tất việc đọc instance cũ.

---

# 21. Hoàn tất rotation

Sau cleanup:

```sql
UPDATE cdc_admin.table_state
SET
    state = 'READY',

    old_capture_instance =
        new_capture_instance,

    new_capture_instance = NULL,

    correlation_id = NULL,

    updated_at = SYSUTCDATETIME()
WHERE source_object_id =
      OBJECT_ID(N'dbo.customer');
```

Kết quả:

```text
Before:

dbo_customer_26082026100000


During migration:

dbo_customer_26082026100000
dbo_customer_27082026163645


After:

dbo_customer_27082026163645
```

Không còn capture instance rác.

---

# 22. Ví dụ end-to-end

## Bước 1 — trạng thái ban đầu

```sql
SELECT *
FROM cdc_admin.table_state
WHERE table_name = 'customer';
```

```text
state = READY
old_capture =
dbo_customer_26082026100000
```

---

## Bước 2 — Developer ALTER

```sql
ALTER TABLE dbo.customer
ADD phone_number VARCHAR(20) NULL;
```

DDL trigger tự động:

```text
correlation_id =
8F0A...

state =
PENDING

queue =
PENDING
```

---

## Bước 3 — Application cố INSERT ngay

```sql
INSERT INTO dbo.customer
(
    customer_id,
    customer_name,
    phone_number
)
VALUES
(
    1001,
    'Nguyen Van A',
    '0901234567'
);
```

DML Guard phát hiện:

```text
state=PENDING
```

kết quả:

```text
CDC_SCHEMA_ROTATION_IN_PROGRESS
```

Transaction bị rollback.

Không có record không đầy đủ đi vào CDC.

---

## Bước 4 — Worker tạo capture

```text
PENDING
 ↓
CREATING
```

Tên mới:

```text
dbo_customer_27082026163645
```

Worker gọi:

```sql
EXEC sys.sp_cdc_enable_table ...;
```

---

## Bước 5 — Validate

Worker xác nhận:

```text
old capture = exists

new capture = exists

phone_number =
exists in new capture
```

Sau đó:

```text
state = DRAINING
```

---

## Bước 6 — Application retry

Application chạy lại INSERT.

Lần này:

```text
DRAINING
   ↓
ALLOW
```

Dữ liệu được commit.

CDC mới chứa:

```text
customer_id
customer_name
phone_number
```

---

## Bước 7 — Debezium

Debezium:

```text
OLD capture
    ↓
drain remaining events
    ↓
switch schema
    ↓
NEW capture
```

Kafka event mới có:

```json
{
  "after": {
    "customer_id": 1001,
    "customer_name": "Nguyen Van A",
    "phone_number": "0901234567"
  }
}
```

---

## Bước 8 — S3 Sink

S3 Sink ghi event tới MinIO.

Kiểm tra object/file mới bảo đảm schema downstream có:

```text
phone_number
```

---

## Bước 9 — Debezium COMPLETED

Notification:

```text
Capture Instance
COMPLETED

dbo_customer_26082026100000
```

---

## Bước 10 — Cleanup

Worker:

```sql
EXEC sys.sp_cdc_disable_table ...
```

Sau cleanup:

```text
dbo_customer_27082026163645
```

duy nhất còn tồn tại.

State:

```text
READY
```

Rotation hoàn tất.

---

# 23. Quy trình vận hành khi deploy schema

Developer/DBA chỉ cần thực hiện:

```sql
ALTER TABLE dbo.customer
ADD phone_number VARCHAR(20);
```

Sau đó **không thao tác thủ công CDC**.

Hệ thống tự thực hiện:

```text
1. Detect ALTER
2. Lock DML
3. Queue request
4. Create capture
5. Validate
6. Unlock DML
7. Debezium migration
8. Wait COMPLETED
9. Delete old capture
10. READY
```

---

# 24. Quy định đối với Developer

## Bắt buộc

Mỗi lần schema change trên cùng một bảng phải hoàn thành rotation trước schema change tiếp theo.

Debezium cũng khuyến nghị hoàn thành toàn bộ quá trình schema update trước khi thực hiện schema update tiếp theo trên cùng source table.

Nên gom:

```sql
ALTER TABLE dbo.customer
ADD
    phone_number VARCHAR(20),
    email VARCHAR(200),
    address NVARCHAR(500);
```

thay vì liên tục:

```sql
ALTER TABLE ... ADD phone_number ...
ALTER TABLE ... ADD email ...
ALTER TABLE ... ADD address ...
```

---

# 25. Những thao tác tuyệt đối không thực hiện

Không chạy thủ công:

```sql
DELETE FROM cdc.change_tables;
```

Không:

```sql
DROP TABLE cdc.xxx_CT;
```

Không xóa instance cũ chỉ vì thấy có hai instance.

Không chạy:

```sql
sp_cdc_disable_table
```

trước khi Debezium báo instance cũ đã hoàn tất.

Không disable toàn bộ CDC database để xử lý một bảng.

Không chạy schema change thứ hai khi migration thứ nhất chưa hoàn tất.

---

# 26. Monitoring

## Kiểm tra bảng đang có hai capture instances

```sql
SELECT
    OBJECT_SCHEMA_NAME(
        ct.source_object_id
    ) AS schema_name,

    OBJECT_NAME(
        ct.source_object_id
    ) AS table_name,

    COUNT(*) AS capture_count
FROM cdc.change_tables ct
GROUP BY ct.source_object_id
HAVING COUNT(*) > 1;
```

Hai instance không phải lỗi nếu:

```text
state = DRAINING
```

Hai instance là bất thường nếu:

```text
state = READY
```

trong thời gian dài.

---

# 27. Kiểm tra queue bị treo

```sql
SELECT *
FROM cdc_admin.rotation_queue
WHERE status NOT IN
(
    'COMPLETED',
    'CANCELLED'
)
ORDER BY created_at;
```

Alert nếu:

```text
PENDING > 30 giây
CREATING > 30 giây
VALIDATING > 30 giây
```

Ngưỡng thực tế cần chỉnh theo môi trường.

---

# 28. Kiểm tra lỗi

```sql
SELECT *
FROM cdc_admin.rotation_log
WHERE event_name = 'ROTATION_ERROR'
ORDER BY created_at DESC;
```

Ngoài log riêng, cần kiểm tra CDC SQL Server:

```sql
SELECT *
FROM sys.dm_cdc_errors
ORDER BY entry_time DESC;
```

và:

```sql
SELECT *
FROM sys.dm_cdc_log_scan_sessions
ORDER BY session_id DESC;
```

---

# 29. Troubleshooting

## Case 1 — state=PENDING quá lâu

Kiểm tra worker.

```sql
SELECT *
FROM cdc_admin.rotation_queue
WHERE status = 'PENDING';
```

Nếu worker chết:

```text
restart cdc-schema-manager
```

Không đổi state thành READY thủ công.

---

## Case 2 — `sp_cdc_enable_table` lỗi

Giữ:

```text
state=ERROR
```

DML tiếp tục bị block.

Kiểm tra:

```text
permission
SQL Agent
CDC status
capture count
capture name length
index configuration
captured column list
```

Sau khi sửa lỗi:

```text
retry request
```

---

## Case 3 — đã tạo new capture nhưng validation lỗi

Không xóa old capture.

Vì DML chưa được release, có thể:

```text
disable new invalid capture
      ↓
fix configuration
      ↓
create another new capture
```

Old capture vẫn giữ an toàn dữ liệu cũ.

---

## Case 4 — có hai capture nhưng Debezium chưa COMPLETED

Không cleanup.

Kiểm tra:

```text
Debezium connector RUNNING?
Kafka Connect task RUNNING?
connector lag?
Kafka notification topic?
```

Giữ:

```text
state = DRAINING
```

---

## Case 5 — Debezium bị stop sau khi DML đã release

Không xóa capture nào.

SQL Server CDC tiếp tục lưu change records.

Khởi động lại Debezium và cho connector tiếp tục.

---

## Case 6 — old capture cleanup thất bại

Không ảnh hưởng dữ liệu mới.

State chuyển:

```text
CLEANUP_PENDING
```

Worker retry:

```text
1 phút
5 phút
15 phút
```

và alert DBA nếu vượt SLA.

---

# 30. Rollback

## Trường hợp chưa release DML

Nếu:

```text
PENDING
CREATING
VALIDATING
ERROR
```

thì chưa có DML mới được commit.

Có thể:

```text
1. giữ old capture
2. disable new capture nếu new đã tạo nhưng invalid
3. sửa lỗi
4. chạy lại rotation
```

Đây là rollback đơn giản nhất.

---

## Trường hợp đã state=DRAINING

Không rollback capture instance mới tùy tiện.

Lúc này đã có DML mới được ghi theo schema mới.

Phải giữ:

```text
OLD + NEW
```

cho tới khi Debezium được xử lý ổn định.

---

# 31. Security và permission matrix cuối cùng

| Principal | Scope | Quyền sử dụng trong SOP |
|---|---|---|
| DBA/deployment | Server + `[SourceDB]` | Cài đặt object; `sysadmin` chỉ cần nếu phải chạy `sp_cdc_enable_db`; quyền tạo trigger/schema/table theo Bước 6 |
| `svc_cdc_rotation` | `[SourceDB]` | `db_owner`; không cấp `sysadmin` |
| Application | `[SourceDB]` source tables | DML nghiệp vụ; không quyền trên `cdc_admin.*`; không `db_owner` |
| Developer/migration | `[SourceDB]` | `ALTER` table theo policy; không quyền CDC admin trực tiếp |
| Debezium | `[SourceDB]` | `SELECT` captured columns + gating role nếu cấu hình; không `db_owner` chỉ để phục vụ rotation |
| Operator read-only | `[SourceDB]` | Có thể cấp `SELECT` trên `cdc_admin` và quyền xem monitoring phù hợp; không quyền sửa state |
| SQL Agent operator | `msdb` | Chỉ áp dụng nếu triển khai worker dưới dạng SQL Agent job |

Nếu dùng SQL Server Agent thay cho worker ngoài, job metadata nằm trong `msdb`. Người tạo/quản lý job không phải `sysadmin` phải thuộc một role SQL Agent phù hợp trong `msdb`, như `SQLAgentUserRole`, `SQLAgentReaderRole` hoặc `SQLAgentOperatorRole`. T-SQL job step chạy theo security context của job owner, vì vậy job owner vẫn phải có quyền cần thiết trong `[SourceDB]` để chạy logic CDC.

Không cấp `db_owner` cho application, developer hoặc Debezium chỉ để phục vụ automation.

Credential Worker phải:

```text
non-interactive
+ secret manager/Vault
+ network restricted
+ audit enabled
+ không dùng chung với Debezium/application
```

---

# 32. Checklist trước khi production

- [ ] Đã xác định chính xác `[SourceDB]`; mọi script CDC/admin đều có `USE [SourceDB]`.
- [ ] Database CDC đang hoạt động.
- [ ] SQL Server Agent/Capture job hoạt động.
- [ ] Debezium connector RUNNING.
- [ ] `cdc_admin` schema đã tạo.
- [ ] `table_state` đã seed.
- [ ] `rotation_queue` hoạt động.
- [ ] Audit log hoạt động.
- [ ] Worker account đã tồn tại và có `db_owner` chỉ trên `[SourceDB]`.
- [ ] Worker service đang RUNNING trước khi enable DDL trigger.
- [ ] Debezium notification topic đã kiểm tra consume thành công trước khi enable DDL trigger.
- [ ] DML guard được cài cho toàn bộ bảng CDC cần bảo vệ.
- [ ] DML Guard sử dụng `WITH EXECUTE AS OWNER`.
- [ ] DDL trigger được tạo sau DML Guard và sử dụng `WITH EXECUTE AS OWNER`.
- [ ] DDL trigger hoạt động và `is_disabled = 0`.
- [ ] Worker sử dụng account riêng, không dùng account Application/Debezium.
- [ ] Naming convention đã thống nhất.
- [ ] Capture name không vượt 100 ký tự.
- [ ] Debezium notification `sink` đã enable.
- [ ] Notification topic đã tồn tại.
- [ ] Worker consume được notification.
- [ ] Alert cho PENDING/ERROR.
- [ ] Alert cho hai capture instances tồn tại quá SLA.
- [ ] Application có retry khi gặp `CDC_SCHEMA_ROTATION_IN_PROGRESS`.
- [ ] Đã kiểm thử Kafka event.
- [ ] Đã kiểm thử S3 Sink.
- [ ] Đã kiểm thử file/object trên MinIO.

---

# 33. Bộ test nghiệm thu bắt buộc

## Test 1 — Normal flow

```sql
ALTER TABLE dbo.customer
ADD phone_number VARCHAR(20);
```

Expected:

```text
READY
→ PENDING
→ CREATING
→ VALIDATING
→ DRAINING
→ READY
```

---

## Test 2 — INSERT ngay sau ALTER

```sql
ALTER TABLE ...
```

ngay sau đó:

```sql
INSERT ...
```

Expected:

```text
INSERT bị block trước khi new capture ready.
```

Sau khi READY/DRAINING:

```text
retry INSERT thành công.
```

---

## Test 3 — Worker bị shutdown

Tắt worker.

Chạy:

```sql
ALTER TABLE ...
```

Expected:

```text
state=PENDING
DML bị block.
```

Start worker.

Expected:

```text
worker tự recovery
→ create
→ validate
→ release DML.
```

---

## Test 4 — Debezium shutdown

Sau khi:

```text
state=DRAINING
```

tắt Debezium.

Expected:

```text
OLD không bị xóa.
```

Start Debezium.

Expected:

```text
Debezium tiếp tục
→ COMPLETED
→ cleanup.
```

---

## Test 5 — ALTER lần hai khi đang migration

Khi tồn tại hai capture instances:

```sql
ALTER TABLE dbo.customer
ADD test_column INT;
```

Expected:

```text
DDL bị reject.
```

---

## Test 6 — Downstream

Sau schema migration:

```sql
INSERT ...
phone_number='0901234567'
```

Kiểm tra:

```text
SQL Server
↓
CDC
↓
Debezium event
↓
Kafka
↓
S3 Sink
↓
MinIO
```

`phone_number` phải tồn tại xuyên suốt.

---

# 34. Tiêu chí nghiệm thu cuối cùng

Giải pháp chỉ được coi là production-ready khi chứng minh được:

```text
ALTER TABLE
       ↓
Không tồn tại DML commit bằng schema mới
trước khi new capture ready
```

và:

```text
Old capture
không bao giờ bị xóa
trước Debezium COMPLETED
```

và:

```text
Sau migration hoàn tất
mỗi bảng chỉ còn
1 capture instance
```

và:

```text
mọi rotation đều trace được bằng
correlation_id
```

Ví dụ trace:

```text
Correlation:
9C52B480-...

DDL_DETECTED
     ↓
ROTATION_STARTED
     ↓
CAPTURE_CREATED
     ↓
CAPTURE_VALIDATED
     ↓
DML_RELEASED
     ↓
DEBEZIUM_COMPLETED
     ↓
OLD_CAPTURE_DISABLED
     ↓
ROTATION_COMPLETED
```

---

# 35. Quy trình rút gọn dành cho Operator

Trong vận hành bình thường, Operator chỉ cần nhớ:

```text
1. Developer chạy ALTER TABLE.
2. Kiểm tra state chuyển PENDING.
3. Worker phải chuyển sang DRAINING.
4. DML được mở lại.
5. Debezium phải phát COMPLETED.
6. Old capture phải bị cleanup.
7. State cuối cùng phải READY.
8. Chỉ còn một capture instance.
```

Nếu bất kỳ bước nào lỗi:

```text
KHÔNG xóa capture instance thủ công.
KHÔNG set READY thủ công.
KHÔNG chạy ALTER tiếp.
```

Kiểm tra:

```text
cdc_admin.rotation_queue
cdc_admin.rotation_log
cdc_admin.table_state
sys.sp_cdc_help_change_data_capture
sys.dm_cdc_errors
sys.dm_cdc_log_scan_sessions
Debezium logs
Debezium notification topic
Kafka Connect status
S3 Sink status
```

---

# 36. Kết luận kiến trúc

Thiết kế production được chuẩn hóa như sau:

```text
ALTER TABLE
     ↓
DDL Trigger
     ↓
PENDING
     ↓
DML Guard = BLOCK
     ↓
CDC Schema Manager
     ↓
New Capture Instance
     ↓
Validation
     ↓
DRAINING
     ↓
DML Guard = ALLOW
     ↓
Debezium schema transition
     ↓
Capture Instance COMPLETED
     ↓
Disable OLD Capture
     ↓
READY
```

Điểm cốt lõi là:

> **Detection có thể bất đồng bộ, nhưng bảo vệ dữ liệu phải đồng bộ.**

DDL trigger chịu trách nhiệm phát hiện. DML guard chịu trách nhiệm bảo vệ khoảng chuyển đổi. Worker chịu trách nhiệm automation. Debezium notification chịu trách nhiệm xác định thời điểm cleanup an toàn.

Cơ chế này loại bỏ khoảng trống của online schema update thông thường, đồng thời vẫn cho phép Debezium, Kafka, S3 Sink và MinIO tiếp tục hoạt động mà không cần restart toàn bộ pipeline mỗi lần thêm column.
# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV: Dương Đức Vương / 2A020602944**
**Repo: https://github.com/VuongVau6565/K4-Track02-Day17-DuongDucVuong-2A202602944-Data-Pipeline-Engineering**
**Commit bài nộp:** `fbf74549564cf474f0b59b1452db9dd4b07cd131` (HEAD hiện tại)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Copilot SDK trong VS Code — hỗ trợ phân tích lỗi, sửa pipeline, cập nhật REPORT và chạy kiểm thử/parity.
**Nguồn tham khảo khác (nếu có):** Không.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 hàng cho 12 ticket; T-91 xuất hiện với trạng thái cũ và mới. | `gold_feature_daily` không khớp full recompute; thiếu events của u05 theo event date 2026-08-12. | T-97 vẫn là ticket live trong Silver, training snapshot mới nhất và RAG chunks dù nguồn đã gửi CDC delete. |
| **Nguyên nhân gốc** | Hàm chỉ khử trùng lặp thay đổi trong từng batch, sau đó `INSERT` mọi batch nên các trạng thái của cùng ticket cùng tồn tại. | Lookback bằng 0 chỉ làm mới ngày ingest; sự kiện đến ngày 15 nhưng có `event_time` ngày 12 không được tính vào phân vùng ngày 12. | Parser chỉ lấy `ticket_id` từ `after`; CDC delete có `after = null`, nên không tìm thấy khóa nằm trong `before`/Kafka `key` và bị loại khỏi staging. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: `MERGE` theo `ticket_id`; chỉ cập nhật hàng hiện tại khi `_lsn` đầu vào lớn hơn `_lsn` đã lưu, để replay batch cũ không ghi đè trạng thái mới. | `pipeline/config.py`: đặt `LOOKBACK_DAYS = 3`; `pipeline/gold.py` lọc và tổng hợp theo `CAST(event_time AS DATE)`, không theo ngày ingest. | `pipeline/staging.py`: lấy khóa bằng `COALESCE(after.ticket_id, before.ticket_id, key.ticket_id)`. Silver giữ tombstone `is_deleted` cùng `_lsn`; Gold loại ticket khỏi snapshot mới nhất và tái tạo RAG chunks chỉ từ ticket live. |
| **Khái niệm trên slide** | Khử trùng lặp trong batch khác với upsert theo khoá xuyên batch; CDC LSN xác định thứ tự trạng thái. | Event time khác ingest time; đo lateness từ Bronze rồi làm tròn P99 lên ngày đủ bao phủ. | CDC delete (`op=d`, có LSN và khóa) là thay đổi dữ liệu; Kafka tombstone (`value=null`) chỉ phục vụ compaction. Tombstone Silver giữ khóa/LSN chống replay cũ làm ticket sống lại. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3` (ceil(3.00) = 3 ngày)
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE giữ một trạng thái theo mỗi khoá ticket; LSN chặn batch cũ ghi đè batch mới, còn overwrite-partition tái tạo đúng phân vùng Gold một cách idempotent.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ khóa và LSN làm bằng chứng delete để replay CDC cũ không hồi sinh ticket, đồng thời xóa PII khỏi hàng hiện tại và lọc khỏi Gold.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: giữ khả năng tái lập theo phiên bản; khi có yêu cầu xoá production, cần quy trình erasure có kiểm toán theo retention thay vì coi snapshot bất biến là ngoại lệ.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: DuckDB/dbt chạy cục bộ trên Bronze nhỏ; `unique_key` + điều kiện LSN làm MERGE an toàn khi replay, còn microbatch ngày với lookback 3 tái tính các ngày sự kiện muộn.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   Trong production, quyền xoá phải ưu tiên: giữ snapshot version bất biến về mặt logic khi còn hợp pháp,
   nhưng thực hiện quy trình erasure có kiểm toán để loại/ẩn PII khỏi bản lưu và bản sao lưu theo chính
   sách retention; dựng lại các snapshot bị ảnh hưởng từ nguồn đã lọc hoặc vô hiệu hoá chúng. Bài lab
   giữ snapshot cũ để minh hoạ bất biến, không phải cơ chế tuân thủ quyền xoá.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

## 5. Output (dán nguyên văn)

```text
$env:PYTHONUTF8 = '1'; .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

.\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 8.01s

$env:PYTHONUTF8 = '1'; .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

.\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$env:DO_NOT_TRACK = '1'
Push-Location dbt_project
try {
    ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
} finally { Pop-Location }
Completed successfully — PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

.\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.

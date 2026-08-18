# Reflection — Lakehouse Anti-Pattern

Anti-pattern đội chúng tôi có nguy cơ cao nhất là **lakehouse không có maintenance, dẫn tới small-file explosion**. Dữ liệu quan sát LLM được ghi liên tục theo từng request; nếu mỗi micro-batch tạo một file, số object tăng nhanh hơn dung lượng thực. NB6 cho thấy 100.000 dòng nhưng có 200 file nhỏ, trung bình chỉ 51,5 KB/file. Sau compaction còn 11 file, giảm 18 lần; clustering tiếp tục cho phép bỏ qua 90% file ở point query.

Điều đáng chú ý là chỉ chạy `VACUUM` hoặc `expire_snapshots` chưa đủ. `VACUUM` không nhìn thấy file do writer crash chưa từng commit, còn Iceberg giảm từ 20 xuống 3 snapshot nhưng ban đầu không xóa manifest nào. Vì vậy team cần vận hành compaction, clustering, expiry và orphan sweep như một workflow có đo lường; đồng thời sửa trigger của writer để không tiếp tục sinh file nhỏ. Chúng tôi sẽ theo dõi file count, kích thước file trung bình, scan amplification và metadata:data ratio, thay vì chỉ theo dõi tổng số GB.

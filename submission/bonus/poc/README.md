# Tokenization PoC

Chạy từ root của repository:

```powershell
.\.venv\Scripts\python.exe .\submission\bonus\poc\tokenization_demo.py
```

PoC chỉ dùng dữ liệu tổng hợp và Python standard library. Key trong file là key
demo; production phải lấy key version từ KMS và không đưa key/plaintext vào audit
log. Script kết thúc với bốn dòng `[PASS]` hoặc non-zero exit nếu vi phạm một
privacy invariant.

`tokenization_demo.ipynb` là bản đã thực thi và giữ output của cùng PoC.

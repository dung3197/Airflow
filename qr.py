import qrcode
import math

file_path = r'C:\Users\Admin\test\code.py'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

chunk_size = 2000
chunks = [
    content[i:i + chunk_size]
    for i in range(0, len(content), chunk_size)
]

total = len(chunks)

for index, chunk in enumerate(chunks, start=1):
    qr_data = f"PART {index}/{total}\n{chunk}"

    img = qrcode.make(qr_data)
    img.save(f"code_part_{index}.png")

print(f"Generated {total} QR codes")

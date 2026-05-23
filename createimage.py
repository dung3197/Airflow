file_path = r'C:\Users\Admin\test\code.py'

# Open the file and read its contents into a text string
with open(file_path, 'r', encoding='utf-8') as file:
    code_as_text = file.read()



import qrcode
# Generate the QR code
img = qrcode.make(code_as_text)

# Save it as an image file
img.save("my_qr_code.png")

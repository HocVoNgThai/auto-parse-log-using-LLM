import google.generativeai as genai
import os
import sys
import time
from dotenv import load_dotenv

# --- Cấu hình API Key sử dụng python-dotenv ---
# Tải các biến môi trường từ tệp .env
load_dotenv()

# Lấy API key từ biến môi trường đã được tải
api_key = os.getenv("GOOGLE_API_KEY")

if not api_key:
    print("Lỗi: Không tìm thấy GOOGLE_API_KEY trong tệp .env hoặc biến môi trường.")
    sys.exit(1)

genai.configure(api_key=api_key)


def translate_to_vietnamese(word_to_translate: str) -> str:
    """
    Hàm gọi API Gemini để dịch một từ sang tiếng Việt.

    Args:
        word_to_translate: Từ cần dịch.

    Returns:
        Bản dịch của từ hoặc thông báo lỗi.
    """
    model = genai.GenerativeModel('gemini-1.5-flash')
    prompt = f"Dịch từ hoặc câu sau sang tiếng Việt: {word_to_translate}. Yêu cầu không thêm giải thích, không nói thêm gì ngoài nghĩa dịch của câu hoặc từ. Nếu 1 từ có nhiều nghĩa, ghi các nghĩa cách nhau bởi dấu phẩy, nhiều nhất liệt kê ba nghĩa."
    
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        return f"Đã xảy ra lỗi khi gọi API: {e}"

def main():
    """
    Hàm chính để chạy chương trình CLI với vòng lặp.
    """
    print("--- CHƯƠNG TRÌNH DỊCH ANH-VIỆT (sử dụng .env) ---")
    print("Nhập 'q' hoặc 'quit' để thoát.")
    print("-" * 50)

    while True:
        input_word = input("Nhập từ cần dịch: ")

        if input_word.lower() in ['q', 'quit', 'exit']:
            print("Cảm ơn đã sử dụng chương trình!")
            break
        
        if not input_word:
            continue

        start_time = time.monotonic()
        translation = translate_to_vietnamese(input_word)
        end_time = time.monotonic()
        duration = end_time - start_time
        
        print(f"  -> Kết quả: {translation}")
        print(f"  -> Thời gian phản hồi: {duration:.2f} giây")
        print("-" * 50)


if __name__ == "__main__":
    main()
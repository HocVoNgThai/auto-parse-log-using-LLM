import subprocess
import time
import re
import os
import argparse

# Sử dụng argparse để truyền tham số linh hoạt hơn
parser = argparse.ArgumentParser(description="Giám sát log của Logstash và kích hoạt cơ chế tự sửa lỗi.")
parser.add_argument("--logfile", default="/var/log/logstash/logstash-plain.log", help="Đường dẫn tới file log của Logstash.")
parser.add_argument("--fixer_script", default="parserlog.py", help="Đường dẫn tới script sửa lỗi.")
parser.add_argument("--definitions_path", default="log_definitions", help="Đường dẫn tới thư mục chứa các định nghĩa log.")
args = parser.parse_args()

# Mẫu regex "Bắt tất cả lỗi"
# Nó tìm kiếm [ERROR] hoặc [FATAL] và cố gắng bắt pipeline_id nếu có
# Ví dụ dòng log khớp: [2025-10-08...][ERROR][logstash.agent   ][main] Failed to execute... pipeline_id:paloalto_traffic
# Hoặc:             [2025-10-08...][FATAL][logstash.runner  ][main] An unhandled error occurred...
CATCH_ALL_ERROR_PATTERN = re.compile(
    r"\[[\d\:\s\.\-,T]+\]\[(?P<log_level>ERROR|FATAL)\].*?(?:\[(?P<pipeline_id_1>[a-zA-Z0-9_-]+)\]|pipeline_id:(?P<pipeline_id_2>[a-zA-Z0-9_-]+))?(?P<message>.*)"
)

# Để tránh spam sửa lỗi liên tục cho cùng một pipeline
COOLDOWN_PERIOD = 300  # Giây (5 phút)
last_triggered = {}

def trigger_fixer(pipeline_id, error_message):
    """Kích hoạt script parserlog.py để sửa lỗi."""
    
    current_time = time.time()
    if pipeline_id in last_triggered and (current_time - last_triggered[pipeline_id]) < COOLDOWN_PERIOD:
        print(f"---  Đang trong thời gian chờ (cooldown) cho pipeline '{pipeline_id}', bỏ qua trigger. ---")
        return

    print(f"\n🔥🔥🔥 PHÁT HIỆN LỖI trong pipeline '{pipeline_id}' 🔥🔥🔥")
    print(f"Nội dung lỗi (tóm tắt): {error_message[:200]}...") # In ra 200 ký tự đầu của lỗi
    print(f"--- Kích hoạt cỗ máy sửa lỗi cho '{pipeline_id}'... ---")

    # Kiểm tra xem định nghĩa cho pipeline này có tồn tại không
    if not os.path.exists(os.path.join(args.definitions_path, pipeline_id)):
        print(f"--- ⚠️  CẢNH BÁO: Không tìm thấy định nghĩa trong '{args.definitions_path}' cho pipeline '{pipeline_id}'. Không thể tự động sửa. ---")
        # Ghi nhận thời gian để không spam cảnh báo này
        last_triggered[pipeline_id] = current_time
        return
        
    try:
        # Gọi script parserlog.py với tham số --fix
        command = ["python3", args.fixer_script, "--fix", pipeline_id, "--error", error_message]
        # Sử dụng run với capture_output để log lại output của script con
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print("--- Output từ cỗ máy sửa lỗi: ---")
        print(result.stdout)
        
        last_triggered[pipeline_id] = current_time
        print(f"--- ✅ Hoàn tất quá trình sửa lỗi cho '{pipeline_id}'. Tiếp tục giám sát... ---")
    except subprocess.CalledProcessError as e:
        print(f"--- ❌ Cỗ máy sửa lỗi đã chạy và gặp lỗi. ---")
        print(f"--- Lỗi từ script con: ---\n{e.stderr}")
    except Exception as e:
        print(f"--- ❌ Lỗi không xác định khi gọi cỗ máy sửa lỗi: {e} ---")


def monitor_log_file():
    """Theo dõi file log của Logstash."""
    print(f"--- 🕵️ Bắt đầu giám sát file log: {args.logfile} ---")
    try:
        process = subprocess.Popen(['tail', '-F', '-n', '0', args.logfile], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        while True:
            line = process.stdout.readline()
            if not line:
                time.sleep(1)
                continue

            match = CATCH_ALL_ERROR_PATTERN.search(line)
            if match:
                error_details = match.groupdict()
                
                # Cố gắng tìm pipeline_id từ 2 vị trí có thể có trong regex
                pipeline_id = error_details.get("pipeline_id_1") or error_details.get("pipeline_id_2") or "main"
                
                error_message = line.strip() # Gửi toàn bộ dòng lỗi để có đầy đủ ngữ cảnh
                
                trigger_fixer(pipeline_id, error_message)
                    
    except KeyboardInterrupt:
        print("\n--- 🛑 Dừng giám sát. ---")
    except Exception as e:
        print(f"\n--- ❌ Lỗi trong quá trình giám sát: {e} ---")

if __name__ == "__main__":
    monitor_log_file()
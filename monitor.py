import subprocess
import time
import re
import os
import argparse

parser = argparse.ArgumentParser(description="Giám sát log của Logstash và kích hoạt cơ chế tự sửa lỗi.")
parser.add_argument("--logfile", default="/var/log/logstash/logstash-plain.log", help="Đường dẫn tới file log của Logstash.")
parser.add_argument("--fixer_script", default="parserlog.py", help="Đường dẫn tới script sửa lỗi.")
parser.add_argument("--definitions_path", default="log_definitions", help="Đường dẫn tới thư mục chứa các định nghĩa log.")
args = parser.parse_args()


IS_ERROR_PATTERN = re.compile(r"\[(ERROR|FATAL)\]")

PIPELINE_ID_PATTERN_1 = re.compile(r"pipeline_id:(?P<pipeline_id>\w+)")
PIPELINE_ID_PATTERN_2 = re.compile(r"\[logstash\..*?\]\[(?P<pipeline_id>\w+)\]")
# ----------------------------------------------------------------

COOLDOWN_PERIOD = 300  # Giây (5 phút)
last_triggered = {}

def trigger_fixer(pipeline_id, error_message):
    """Kích hoạt script parserlog.py để sửa lỗi."""
    
    current_time = time.time()
    if pipeline_id in last_triggered and (current_time - last_triggered[pipeline_id]) < COOLDOWN_PERIOD:
        print(f"---  Đang trong thời gian chờ (cooldown) cho pipeline '{pipeline_id}', bỏ qua trigger. ---")
        return

    print(f"\n🔥🔥🔥 PHÁT HIỆN LỖI trong pipeline '{pipeline_id}' 🔥🔥🔥")
    print(f"Nội dung lỗi (tóm tắt): {error_message[:200]}...")
    print(f"--- Kích hoạt cỗ máy sửa lỗi cho '{pipeline_id}'... ---")

    if not os.path.exists(os.path.join(args.definitions_path, pipeline_id)):
        print(f"--- ⚠️  CẢNH BÁO: Không tìm thấy định nghĩa trong '{args.definitions_path}' cho pipeline '{pipeline_id}'. Không thể tự động sửa. ---")
        last_triggered[pipeline_id] = current_time
        return
        
    try:
        command = ["python3", args.fixer_script, "--fix", pipeline_id, "--error", error_message]
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

            # BƯỚC 1: KIỂM TRA XEM CÓ LỖI KHÔNG
            if IS_ERROR_PATTERN.search(line):
                
                # BƯỚC 2: NẾU CÓ LỖI, CỐ GẮNG TÌM PIPELINE_ID
                pipeline_id = "main" # Giá trị mặc định
                
                match1 = PIPELINE_ID_PATTERN_1.search(line)
                match2 = PIPELINE_ID_PATTERN_2.search(line)

                if match1:
                    pipeline_id = match1.group("pipeline_id")
                elif match2:
                    pipeline_id = match2.group("pipeline_id")
                
                error_message = line.strip()
                trigger_fixer(pipeline_id, error_message)
                    
    except KeyboardInterrupt:
        print("\n--- 🛑 Dừng giám sát. ---")
    except Exception as e:
        print(f"\n--- ❌ Lỗi trong quá trình giám sát: {e} ---")

if __name__ == "__main__":
    monitor_log_file()
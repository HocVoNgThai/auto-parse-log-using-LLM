import google.generativeai as genai
import subprocess
import os
import re
import time
import json
import sys
from dotenv import load_dotenv
import argparse

def initialize_env(log_type):
    """Tải file .env dựa trên loại log được chỉ định."""
    print(f"--- Đang khởi tạo môi trường cho loại log: {log_type} ---")
    base_dir = "log_definitions"
    config_dir = os.path.join(base_dir, log_type)
    env_path = os.path.join(config_dir, ".env")
    
    if not os.path.exists(env_path):
        print(f"LỖI: Không tìm thấy file định nghĩa '{env_path}'.")
        print(f"Hãy chắc chắn thư mục '{config_dir}' và file .env bên trong nó tồn tại.")
        return False
    
    # Tải các biến môi trường từ file .env được chỉ định
    load_dotenv(dotenv_path=env_path)
    return True

def generate_logstash_config(log_sample, desired_output, input_config, output_config, log_schema, filter_rules, log_type_name, existing_code=None, error_message=None):
    """Gửi yêu cầu đến AI để tạo hoặc sửa code Logstash HOÀN CHỈNH."""
    
    if error_message:
        prompt = f"""The following Logstash configuration for {log_type_name} failed. Please fix the logic inside the 'filter' block based on the error message, strictly following the rules below.
        FILTER RULES:{filter_rules}
        Log Schema (for context): {log_schema}
        Faulty Logstash Code: ```groovy\n{existing_code}\n```
        Concise Error Message: {error_message}
        CRITICAL INSTRUCTION: You MUST provide the entire, complete, and runnable Logstash configuration code."""
    else:
        prompt = f"""Write a complete Logstash configuration for {log_type_name} logs.
        1. The 'input' block must be: {input_config}
        2. The 'filter' block must strictly follow these rules:{filter_rules}
        3. The 'output' block must be: {output_config}
        Log Schema: --- {log_schema} ---
        Sample Raw Log: {log_sample}
        Desired JSON Output (for field names reference): {desired_output}
        CRITICAL INSTRUCTION: You MUST provide the entire, complete, and runnable Logstash configuration code inside a single markdown block."""

    print(f"--- 🤖 Đang gửi yêu cầu (cho {log_type_name}) đến AI... ---")
    try:
        # Lấy API key từ biến môi trường đã được tải
        api_key = os.getenv('GOOGLE_API_KEY')
        if not api_key:
            print("LỖI: GOOGLE_API_KEY không được thiết lập.")
            return None
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        response = model.generate_content(prompt)
        text_response = response.text
        match = re.search(r"```(?:groovy|json|logstash|ruby)?\s*(.*?)\s*```", text_response, re.DOTALL)
        if match:
            code = match.group(1).strip()
            print("--- ✨ Trích xuất code thành công từ khối markdown. ---")
        else:
            print("--- ⚠️ Không tìm thấy khối markdown, sử dụng toàn bộ phản hồi. ---")
            code = text_response.strip()

        if code.startswith("logstash"):
            print("--- 🧹 Phát hiện và loại bỏ chữ 'logstash' thừa ở đầu code. ---")
            code = re.sub(r'^\s*logstash\s*', '', code)
        return code
    except Exception as e:
        print(f"Lỗi khi gọi API của Google: {e}")
        return None

def test_logstash_config(full_config_code, log_sample):
    test_config_code = re.sub(r'input\s*\{.*\}', 'input { stdin {} }', full_config_code, flags=re.DOTALL)
    test_config_code = re.sub(r'output\s*\{.*\}', 'output { stdout { codec => json_lines } }', test_config_code, flags=re.DOTALL)
    temp_filename = "/tmp/temp_test_logstash.conf"
    with open(temp_filename, "w", encoding='utf-8') as f: f.write(test_config_code)
    command = ["sudo", "-u", "logstash", "/usr/share/logstash/bin/logstash", "-f", temp_filename, "--path.settings", "/etc/logstash"]
    print(f"--- ⚙️ Đang thực thi KIỂM THỬ với lệnh: {' '.join(command)} ---")
    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout, stderr = process.communicate(input=log_sample)
        exit_code = process.returncode
        os.remove(temp_filename)
        return stdout, stderr, exit_code
    except Exception as e:
        if os.path.exists(temp_filename): os.remove(temp_filename)
        print(f"Đã xảy ra lỗi khi chạy subprocess của Logstash: {e}")
        return None, str(e), 1

def deploy_config_and_restart_logstash(config_code, destination_path):
    print(f"--- 🚀 Bắt đầu quá trình triển khai cấu hình mới ---")
    try:
        temp_filename = "/tmp/final_config.conf"
        with open(temp_filename, "w", encoding='utf-8') as f: f.write(config_code)
        print(f"Bước 1: Di chuyển file cấu hình từ {temp_filename} đến '{destination_path}'...")
        subprocess.run(["sudo", "mv", temp_filename, destination_path], check=True, capture_output=True)
        print(f"Bước 2: Gán quyền sở hữu cho user 'logstash'...")
        subprocess.run(["sudo", "chown", "logstash:logstash", destination_path], check=True, capture_output=True)
        print("--- ✅ Đã lưu và gán quyền thành công. ---")
        print("Bước 3: Khởi động lại service Logstash (systemctl restart)...")
        subprocess.run(["sudo", "systemctl", "restart", "logstash"], check=True, capture_output=True)
        print("--- ✅ Lệnh khởi động lại đã được gửi. ---")
        print("Bước 4: Đợi 5 giây để service khởi động...")
        time.sleep(5)
        print("Kiểm tra trạng thái service Logstash...")
        status_check = subprocess.run(["sudo", "systemctl", "is-active", "--quiet", "logstash"])
        if status_check.returncode == 0:
            print("--- ✅✅✅ TUYỆT VỜI! Service Logstash đang 'active (running)' với cấu hình mới. ---")
            print("--- Bạn có thể xem log bằng lệnh: sudo journalctl -u logstash -f ---")
        else:
            print("--- ❌❌❌ CẢNH BÁO: Logstash service đã KHÔNG thể khởi động thành công sau khi restart. ---")
            print("--- Hãy kiểm tra log chi tiết bằng lệnh: sudo journalctl -u logstash ---")
        return True
    except subprocess.CalledProcessError as e:
        error_output = e.stderr.decode('utf-8') if e.stderr else str(e)
        print(f"--- ❌ LỖI trong quá trình triển khai. ---")
        print(f"Lệnh thất bại: {' '.join(e.cmd)}")
        print(f"Lỗi chi tiết: {error_output}")
        return False
    except Exception as e:
        print(f"Đã xảy ra lỗi không xác định: {e}")
        return False

def main():
    """Hàm chính, có thể chạy ở 2 chế độ: tạo mới hoặc sửa lỗi."""
    
    parser = argparse.ArgumentParser(description="Tự động tạo và sửa lỗi cấu hình Logstash.")
    parser.add_argument("log_type", help="Tên của loại log cần xử lý (phải trùng với tên thư mục trong log_definitions).")
    parser.add_argument("--fix", action="store_true", help="Chạy ở chế độ sửa lỗi. Sẽ đọc file config hiện tại.")
    parser.add_argument("--error", help="Thông báo lỗi được cung cấp bởi bộ giám sát.")
    args = parser.parse_args()

    log_type_to_process = args.log_type
    initial_error = args.error

    if not initialize_env(log_type_to_process):
        return

    # --- LẤY CÁC BIẾN CẤU HÌNH TỪ .env ĐÃ ĐƯỢC TẢI ---
    log_type_name = os.getenv("LOG_TYPE_NAME", log_type_to_process)
    log_schema = os.getenv("LOG_SCHEMA")
    log_sample = os.getenv("LOG_SAMPLE")
    desired_output = os.getenv("LOG_DESIRED_JSON")
    log_input_path = os.getenv("LOG_INPUT_FILE_PATH")
    final_config_path = os.getenv("LOGSTASH_CONFIG_PATH")
    es_index_prefix = os.getenv("ELASTICSEARCH_INDEX_PREFIX")
    es_hosts = os.getenv("ELASTICSEARCH_HOSTS")
    log_filter_rules = os.getenv("LOG_FILTER_RULES")

    # Kiểm tra các biến quan trọng
    required_vars = {
        "LOG_SCHEMA": log_schema, "LOG_SAMPLE": log_sample, "LOG_DESIRED_JSON": desired_output,
        "LOGSTASH_CONFIG_PATH": final_config_path,
        "ELASTICSEARCH_INDEX_PREFIX": es_index_prefix, "ELASTICSEARCH_HOSTS": es_hosts,
        "LOG_FILTER_RULES": log_filter_rules,
    }
    # LOG_INPUT_FILE_PATH is optional if LOG_INPUT_CONFIG is used
    if not os.getenv("LOG_INPUT_CONFIG") and not log_input_path:
        required_vars["LOG_INPUT_FILE_PATH"] = log_input_path

    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        print(f"LỖI: Các biến sau không được định nghĩa trong file .env: {', '.join(missing_vars)}")
        return
        
    print(f"--- Đang chạy cho loại log: {log_type_name} ---")

    # Xây dựng cấu hình input/output từ các biến
    input_config = os.getenv("LOG_INPUT_CONFIG")
    if not input_config: # Fallback to file input if full config not provided
        input_config = f"""file {{ path => "{log_input_path}" start_position => "beginning" }}"""
        
    es_index = f"{es_index_prefix}-%{{+YYYY.MM.dd}}"
    output_config = f"""elasticsearch {{ hosts => {es_hosts} index => "{es_index}" }}"""
    # -----------------------------------------------

    status_check = subprocess.run(["sudo", "systemctl", "is-active", "--quiet", "logstash"])
    if status_check.returncode == 0:
        print("--- ⚠️ Service Logstash đang chạy. Sẽ tạm thời dừng service để bắt đầu quá trình tạo config mới. ---")
        try:
            subprocess.run(["sudo", "systemctl", "stop", "logstash"], check=True, capture_output=True)
            print("--- ✅ Service Logstash đã được dừng tạm thời. ---")
        except subprocess.CalledProcessError as e:
            error_output = e.stderr.decode('utf-8') if e.stderr else str(e)
            print(f"--- ❌ Không thể dừng service Logstash. Vui lòng kiểm tra quyền sudo. Lỗi: {error_output} ---")
            return
    else:
        print("--- Service Logstash hiện không chạy. Bắt đầu quá trình... ---")
    
    existing_code = None
    if args.fix:
        print(f"--- 🏃 Chạy ở chế độ SỬA LỖI cho file: {final_config_path} ---")
        try:
            with open(final_config_path, 'r', encoding='utf-8') as f:
                existing_code = f.read()
            print("--- ✅ Đã đọc thành công file config bị lỗi. ---")
        except FileNotFoundError:
            print(f"--- ⚠️ Không tìm thấy file config '{final_config_path}'. Chuyển sang chế độ tạo mới. ---")
            initial_error = None # Không có file cũ thì không thể sửa lỗi
            args.fix = False # Tắt chế độ fix
            
    max_retries = 5
    current_code = existing_code
    error_message = initial_error # Bắt đầu vòng lặp với lỗi được cung cấp (nếu có)
    
    for i in range(max_retries):
        print(f"\n--- VÒNG LẶP {i + 1}/{max_retries} ---")
        current_code = generate_logstash_config(log_sample, desired_output, input_config, output_config, log_schema, log_filter_rules, log_type_name, current_code, error_message)
        if current_code:
            print("\n--- 📄 Code do AI tạo ra trong lần lặp này: ---")
            print(current_code)
            print("-------------------------------------------\n")
        else:
            print("--- ❌ AI không trả về code. Dừng vòng lặp. ---")
            break
        
        stdout, stderr, exit_code = test_logstash_config(current_code, log_sample)
        
        if exit_code != 0:
            print(f"--- ❌ Logic filter thất bại (Exit Code: {exit_code}). Chuẩn bị gửi lại cho AI... ---")
            if stderr:
                error_lines = [line for line in stderr.splitlines() if "[INFO ]" not in line and "[WARN ]" not in line]
                concise_error = "\n".join(error_lines[:5])
                print("--- Lỗi tóm tắt gửi cho AI: ---\n" + concise_error)
                error_message = concise_error
            else:
                error_message = "Logstash exited with a non-zero status code but no stderr output."
        elif not stdout:
            print("--- ⚠️ Logic filter không tạo ra output. Coi như lỗi. ---")
            error_message = "Logstash ran successfully but produced no output. The filter might have dropped the event."
        else:
            print("\n--- ✅ THÀNH CÔNG! Logic filter đã chính xác. ---")
            deploy_config_and_restart_logstash(current_code, final_config_path)
            return
            
    print(f"\n--- ❌ Thất bại sau {max_retries} lần thử. Không thể tạo và triển khai cấu hình. ---")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("LỖI: Vui lòng chỉ định loại log cần xử lý.")
        print("Cú pháp: python3 parserlog.py <tên_thư_muc_log>")
        print("Ví dụ:  python3 parserlog.py paloalto_traffic")
    elif not os.getenv('GOOGLE_API_KEY', default=load_dotenv(os.path.join("log_definitions", sys.argv[1], ".env")) and os.getenv('GOOGLE_API_KEY')):
        # Thử tải key từ .env chung nếu có, sau đó thử tải từ .env chuyên dụng
        load_dotenv()
        if not os.getenv('GOOGLE_API_KEY'):
             print("LỖI: Biến GOOGLE_API_KEY không tìm thấy. Vui lòng kiểm tra các file .env.")
        else:
             main()
    else:
        main()
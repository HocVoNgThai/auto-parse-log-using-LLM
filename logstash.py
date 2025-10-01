import google.generativeai as genai
import subprocess
import os


GOOGLE_API_KEY = 'YOUR_GOOGLE_API_KEY' 
# -------------------------

# Cấu hình API
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel('gemini-pro')

def generate_logstash_config(log_sample, desired_output, existing_code=None, error_message=None):
    """Gửi yêu cầu đến AI để tạo hoặc sửa code Logstash."""
    if error_message:
        prompt = f"""
        The following Logstash filter code failed to parse the raw log into the desired JSON output.
        Please fix the code based on the error message. The most likely filter to use is the 'csv' filter.

        Raw Log Sample:
        {log_sample}

        Desired JSON Output:
        {desired_output}

        Faulty Logstash Code:
        ```groovy
        {existing_code}
        ```

        Error Message:
        {error_message}

        Provide only the corrected, complete Logstash configuration code block.
        """
    else:
        prompt = f"""
        Write a complete Logstash configuration that parses the following raw CSV log sample to match the desired JSON output.
        The input is stdin and the output should be stdout with the 'json_lines' codec. Use the 'csv' filter.

        Raw Log Sample:
        {log_sample}

        Desired JSON Output:
        {desired_output}

        Provide only the full Logstash configuration code block.
        """

    print("--- 🤖 Đang gửi yêu cầu đến AI... ---")
    try:
        response = model.generate_content(prompt)
        # Trích xuất code từ trong khối markdown ```
        code = response.text.strip().replace("```groovy", "").replace("```json", "").replace("```", "").strip()
        return code
    except Exception as e:
        print(f"Lỗi khi gọi API của Google: {e}")
        return None


def test_logstash_config(config_code, log_sample):
    """Chạy Logstash với code và log sample, trả về kết quả và lỗi."""
    config_filename = "temp_logstash.conf"
    with open(config_filename, "w", encoding='utf-8') as f:
        f.write(config_code)

    # Câu lệnh để chạy Logstash, đọc config từ file
    command = [
        "logstash",
        "-f",
        config_filename
    ]
    
    print(f"--- ⚙️ Đang thực thi Logstash với file cấu hình '{config_filename}'... ---")
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8'
        )
        
        # Gửi log sample vào stdin và lấy kết quả
        stdout, stderr = process.communicate(input=log_sample)
        
        # Xóa file config tạm
        os.remove(config_filename)

        return stdout, stderr
    except FileNotFoundError:
        print("\n[LỖI] Lệnh 'logstash' không tìm thấy.")
        print("Hãy chắc chắn rằng Logstash đã được cài đặt và đường dẫn của nó nằm trong biến môi trường PATH của bạn.")
        return None, "Logstash not found"
    except Exception as e:
        print(f"Đã xảy ra lỗi khi chạy subprocess của Logstash: {e}")
        return None, str(e)


def main():
    """Hàm chính điều phối vòng lặp tự sửa lỗi."""
    log_sample = '2025/07/24 14:34:06,013201036611,TRAFFIC,end,2562,2025/07/24 14:34:06,14.225.209.154,210.211.104.250,14.225.209.154,210.211.104.250,VPN_GP_in_VietNam_NuocNgoai,,,incomplete,vsys1,EDGE,EDGE,ethernet1/2,ethernet1/2,Forward_SysLog_127.11,2260758,1,36523,80,36523,28869,0x400019,tcp,allow,152,78,74,2,2025/07/24 14:33:54,0,any,,7360121242230814409,0x8000000000000000,Viet Nam,Viet Nam,,1,1,aged-out,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2025-07-24T14:34:07.730+07:00,,,unknown,unknown,unknown,1,,,incomplete,no,no,0'
    
    desired_output = """
{
    "Action_Flags": "0x8000000000000000",
    "Flags": "0x400019",
    "Dest_Zone": "EDGE",
    "@timestamp": "2025-09-23T03:41:36.450037117Z",
    "Device_Name": "PAN-IDC-5220-01",
    "Action_Src": "from-policy",
    "Category": "any",
    "Src_Location": "Viet Nam",
    "Virtual_System": "vsys1",
    "Src_IP": "14.225.209.154",
    "Dest_Port": 80,
    "Session_End_Reason": "aged-out",
    "Log_Action": "Forward_SysLog_127.11",
    "Session_ID": 2260758,
    "type": "pa_traffic",
    "Src_IP_Nat": "14.225.209.154",
    "Bytes_Received": 74,
    "Src_Zone": "EDGE",
    "Bytes_Sent": 78,
    "Rule_Name": "VPN_GP_in_VietNam_NuocNgoai",
    "Dest_Location": "Viet Nam",
    "Application Category": "unknown",
    "Dest_IP_Nat": "210.211.104.250",
    "Application Technology": "unknown",
    "Dest_Port_Nat": 28869,
    "Action": "allow",
    "Application Risk": "1",
    "Dest_IP": "210.211.104.250",
    "Packets": 2,
    "Generated_Time": "2025/07/24 14:34:06",
    "Application Characteristic": null,
    "Protocol": "tcp",
    "Bytes": 152,
    "Src_Port_Nat": 36523,
    "App": "incomplete",
    "Src_Port": 36523,
    "Application Subcategory": "unknown"
}
"""
    
    max_retries = 5
    current_code = None
    error_message = None

    for i in range(max_retries):
        print(f"\n--- VÒNG LẶP {i + 1}/{max_retries} ---")
        
        current_code = generate_logstash_config(log_sample, desired_output, current_code, error_message)
        if not current_code:
            print("--- ❌ Không thể tạo code từ AI, dừng chương trình. ---")
            break

        print("--- 📄 Code do AI tạo ra: ---")
        print(current_code)
        
        stdout, stderr = test_logstash_config(current_code, log_sample)
        
        if stderr is None: # Lỗi không chạy được Logstash
            break

        # Điều kiện kiểm tra lỗi có thể cần chi tiết hơn
        if stderr and ("Error" in stderr or "ERROR" in stderr or "exception" in stderr):
            print("--- ❌ Logstash thất bại. Chuẩn bị gửi lại cho AI... ---")
            print("--- Thông báo lỗi: ---")
            print(stderr)
            error_message = stderr
        elif not stdout:
            print("--- ⚠️ Logstash không tạo ra output. Coi như lỗi. Chuẩn bị gửi lại cho AI... ---")
            error_message = "Logstash ran successfully but produced no output. The filter might have dropped the event."
        else:
            print("\n--- ✅ THÀNH CÔNG! Cấu hình Logstash đã chạy đúng. ---")
            print("\n--- 📜 Code cuối cùng: ---")
            print(current_code)
            print("\n--- ✨ Kết quả JSON đã parse: ---")
            print(stdout)
            return

    print(f"\n--- ❌ Thất bại sau {max_retries} lần thử. Không thể tạo cấu hình hoạt động. ---")


if __name__ == "__main__":
    if GOOGLE_API_KEY == 'YOUR_GOOGLE_API_KEY':
        print("LỖI: Vui lòng thay thế 'YOUR_GOOGLE_API_KEY' bằng API Key của bạn trong file script.")
    else:
        main()
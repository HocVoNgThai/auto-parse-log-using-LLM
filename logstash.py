import google.generativeai as genai
import subprocess
import os
import re
import time

GOOGLE_API_KEY = 'YOUR_GOOGLE_API_KEY'

genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

def generate_logstash_config(log_sample, desired_output, input_config, output_config, log_schema, existing_code=None, error_message=None):
    """Gửi yêu cầu đến AI để tạo hoặc sửa code Logstash HOÀN CHỈNH."""
    
    prompt_rules = f"""
    Your primary task is to create a `filter` block that performs these actions IN ORDER:
    1. Use `if [message] =~ /^Receive Time,Serial Number/ {{ drop {{}} }}` to skip the header row.
    2. Use the `csv` filter to parse the message directly into top-level fields. DO NOT use the `target` option. The column names are defined in the schema.
    3. Use a `mutate` filter to perform `rename` and `convert` operations to match the desired field names and types. You can also `remove_field` for fields that are obviously not needed.
    4. Use a `geoip` filter with source is `Dest_IP` and target is field `Dest_IP_GeoIP` and database is `/etc/logstash/GeoLite2-City.mmdb` to show the location of Destination IP
    5. CRITICAL: DO NOT remove or modify the default `@timestamp` field.
    """

    if error_message:
        prompt = f"""
        The following Logstash configuration failed. Please fix the logic inside the 'filter' block based on the error message, strictly following the rules below.
        
        FILTER RULES:{prompt_rules}
        
        Palo Alto Traffic Log Schema (for context): {log_schema}
        Faulty Logstash Code: ```groovy\n{existing_code}\n```
        Concise Error Message: {error_message}
        CRITICAL INSTRUCTION: You MUST provide the entire, complete, and runnable Logstash configuration code.
        """
    else:
        prompt = f"""
        Write a complete Logstash configuration.
        1. The 'input' block must be: {input_config}
        2. The 'filter' block must strictly follow these rules:{prompt_rules}
        3. The 'output' block must be: {output_config}
        
        Palo Alto Log Schema: --- {log_schema} ---
        Sample Raw Log: {log_sample}
        Desired JSON Output (for field names reference): {desired_output}
        CRITICAL INSTRUCTION: You MUST provide the entire, complete, and runnable Logstash configuration code inside a single markdown block.
        """

    print("--- Đang gửi yêu cầu (với filter đơn giản hóa) đến AI... ---")
    try:
        response = model.generate_content(prompt)
        text_response = response.text
        match = re.search(r"```(?:groovy|json|logstash|ruby)?\s*(.*?)\s*```", text_response, re.DOTALL)
        
        if match:
            code = match.group(1).strip()
            print("--- Trích xuất code thành công từ khối markdown. ---")
        else:
            print("--- Không tìm thấy khối markdown, sử dụng toàn bộ phản hồi. ---")
            code = text_response.strip()

        if code.startswith("logstash"):
            print("--- Phát hiện và loại bỏ chữ 'logstash' thừa ở đầu code. ---")
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
    print(f"--- Đang thực thi KIỂM THỬ với lệnh: {' '.join(command)} ---")
    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout, stderr = process.communicate(input=log_sample)
        exit_code = process.returncode
        os.remove(temp_filename)
        return stdout, stderr, exit_code
    except Exception as e:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
        print(f"Đã xảy ra lỗi khi chạy subprocess của Logstash: {e}")
        return None, str(e), 1

def deploy_config_and_restart_logstash(config_code, destination_path):
    print(f"--- Bắt đầu quá trình triển khai cấu hình mới ---")
    try:
        temp_filename = "/tmp/final_config.conf"
        with open(temp_filename, "w", encoding='utf-8') as f:
            f.write(config_code)
        
        print(f"Bước 1: Di chuyển file cấu hình từ {temp_filename} đến '{destination_path}'...")
        subprocess.run(["sudo", "mv", temp_filename, destination_path], check=True, capture_output=True)
        
        print(f"Bước 2: Gán quyền sở hữu cho user 'logstash'...")
        subprocess.run(["sudo", "chown", "logstash:logstash", destination_path], check=True, capture_output=True)
        print("--- Đã lưu và gán quyền thành công. ---")

        print("Bước 3: Khởi động lại service Logstash (systemctl restart)...")
        subprocess.run(["sudo", "systemctl", "restart", "logstash"], check=True, capture_output=True)
        print("--- Lệnh khởi động lại đã được gửi. ---")
        
        print("Bước 4: Đợi 5 giây để service khởi động...")
        time.sleep(5)
        print("Kiểm tra trạng thái service Logstash...")
        
        status_check = subprocess.run(["sudo", "systemctl", "is-active", "--quiet", "logstash"])
        
        if status_check.returncode == 0:
            print("--- Service Logstash đang 'active (running)' với cấu hình mới. ---")
            print("--- Bạn có thể xem log bằng lệnh: sudo journalctl -u logstash -f ---")
        else:
            print("--- CẢNH BÁO: Logstash service đã KHÔNG thể khởi động thành công sau khi restart. ---")
            print("--- Hãy kiểm tra log chi tiết bằng lệnh: sudo journalctl -u logstash ---")
        
        return True

    except subprocess.CalledProcessError as e:
        error_output = e.stderr.decode('utf-8') if e.stderr else str(e)
        print(f"--- LỖI trong quá trình triển khai. ---")
        print(f"Lệnh thất bại: {' '.join(e.cmd)}")
        print(f"Lỗi chi tiết: {error_output}")
        return False
    except Exception as e:
        print(f"Đã xảy ra lỗi không xác định: {e}")
        return False

def main():
    """Hàm chính điều phối vòng lặp tự sửa lỗi."""
    print("--- Bắt đầu quy trình tự động tạo cấu hình Logstash ---")
    
    status_check = subprocess.run(["sudo", "systemctl", "is-active", "--quiet", "logstash"])
    if status_check.returncode == 0:
        print("--- Service Logstash đang chạy. Sẽ tạm thời dừng service để bắt đầu quá trình tạo config mới. ---")
        try:
            subprocess.run(["sudo", "systemctl", "stop", "logstash"], check=True, capture_output=True)
            print("--- Service Logstash đã được dừng tạm thời. ---")
        except subprocess.CalledProcessError as e:
            error_output = e.stderr.decode('utf-8') if e.stderr else str(e)
            print(f"--- Không thể dừng service Logstash. Vui lòng kiểm tra quyền sudo. Lỗi: {error_output} ---")
            return
    else:
        print("--- Service Logstash hiện không chạy. Bắt đầu quá trình... ---")

    palo_alto_log_schema = "Receive Time,Serial Number,Type,Threat/Content Type,Future_Use,Generated Time,Source Address,Destination Address,NAT Source IP,NAT Destination IP,Rule Name,Source User,Destination User,Application,Virtual System,Source Zone,Destination Zone,Inbound Interface,Outbound Interface,Log Action,Future_Use,Session ID,Repeat Count,Source Port,Destination Port,NAT Source Port,NAT Destination Port,Flags,Protocol,Action,Bytes,Bytes Sent,Bytes Received,Packets,Start Time,Elapsed Time,Category,Future_Use,Sequence Number,Action Flags,Source Country,Destination Country,Future_Use,Packets Sent,Packets Received,Session End Reason,Device Group Hierarchy Level 1,Device Group Hierarchy Level 2,Device Group Hierarchy Level 3,Device Group Hierarchy Level 4,Virtual System Name,Device Name,Action Source,Source VM UUID,Destination VM UUID,Tunnel ID/IMSI,Monitor Tag/IMEI,Parent Session ID,Parent Start Time,Tunnel Type,SCTP Association ID,SCTP Chunks,SCTP Chunks Sent,SCTP Chunks Received,Rule UUID,HTTP/2 Connection,App Flap Count,Policy ID,Link Switches,SD-WAN Cluster,SD-WAN Device Type,SD-WAN Cluster Type,SD-WAN Site,Dynamic User Group Name,XFF Address,Source Device Category,Source Device Profile,Source Device Model,Source Device Vendor,Source Device OS Family,Source Device OS Version,Source Hostname,Source Mac Address,Destination Device Category,Destination Device Profile,Destination Device Model,Destination Device Vendor,Destination Device OS Family,Destination Device OS Version,Destination Hostname,Destination Mac Address,Container ID,POD Namespace,POD Name,Source External Dynamic List,Destination External Dynamic List,Host ID,Serial Number 2,Source Dynamic Address Group,Destination Dynamic Address Group,Session Owner,High Resolution Timestamp,A Slice Service Type,A Slice Differentiator,Application Subcategory,Application Category,Application Technology,Application Risk,Application Characteristic,Application Container,Tunled Application,Application SaaS,Application Sanctioned State,Offloaded"
    final_config_path = "/etc/logstash/conf.d/10-paloalto-autogen.conf"
    input_config = f"""file {{ path => "/var/log/PAN_114.csv" start_position => "beginning" }}"""
    es_hosts = ["http://10.81.89.131:9200"]
    es_index = "paloalto-traffic-%{{+YYYY.MM.dd}}"
    output_config = f"""elasticsearch {{ hosts => {es_hosts} index => "{es_index}" }}"""
    log_sample = '2025/07/24 14:34:06,013201036611,TRAFFIC,end,2562,2025/07/24 14:34:06,14.225.209.154,210.211.104.250,14.225.209.154,210.211.104.250,VPN_GP_in_VietNam_NuocNgoai,,,incomplete,vsys1,EDGE,EDGE,ethernet1/2,ethernet1/2,Forward_SysLog_127.11,2260758,1,36523,80,36523,28869,0x400019,tcp,allow,152,78,74,2,2025/07/24 14:33:54,0,any,,7360121242230814409,0x8000000000000000,Viet Nam,Viet Nam,,1,1,aged-out,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2025-07-24T14:34:07.730+07:00,,,unknown,unknown,unknown,1,,,incomplete,no,no,0,incomplete'
    desired_output = '{"Action_Flags": "0x8000000000000000", "Flags": "0x400019", "Dest_Zone": "EDGE", "Device_Name": "PAN-IDC-5220-01", "Action_Src": "from-policy", "Category": "any", "Src_Location": "Viet Nam", "Virtual_System": "vsys1", "Src_IP": "14.225.209.154", "Dest_Port": 80, "Session_End_Reason": "aged-out", "Log_Action": "Forward_SysLog_127.11", "Session_ID": "2260758", "Src_IP_Nat": "14.225.209.154", "Bytes_Received": 74, "Src_Zone": "EDGE", "Bytes_Sent": 78, "Rule_Name": "VPN_GP_in_VietNam_NuocNgoai", "Dest_Location": "Viet Nam", "Dest_IP_Nat": "210.211.104.250", "Action": "allow", "Dest_IP": "210.211.104.250", "Packets": 2, "Generated_Time": "2025/07/24 14:34:06", "Protocol": "tcp", "Bytes": 152, "Src_Port_Nat": 36523, "Application": "incomplete", "Src_Port": 36523}'
    
    max_retries = 5
    current_code = None
    error_message = None
    for i in range(max_retries):
        print(f"\n--- VÒNG LẶP {i + 1}/{max_retries} ---")
        current_code = generate_logstash_config(log_sample, desired_output, input_config, output_config, palo_alto_log_schema, current_code, error_message)
        
        if current_code:
            print("\n--- Code do AI tạo ra trong lần lặp này: ---")
            print(current_code)
            print("-------------------------------------------\n")
        else:
            print("--- AI không trả về code. Dừng vòng lặp. ---")
            break
        
        stdout, stderr, exit_code = test_logstash_config(current_code, log_sample)
        
        if exit_code != 0:
            print(f"--- Logic filter thất bại (Exit Code: {exit_code}). Chuẩn bị gửi lại cho AI... ---")
            if stderr:
                error_lines = [line for line in stderr.splitlines() if "[INFO ]" not in line and "[WARN ]" not in line]
                concise_error = "\n".join(error_lines[:5])
                print("--- Lỗi tóm tắt gửi cho AI: ---\n" + concise_error)
                error_message = concise_error
            else:
                error_message = "Logstash exited with a non-zero status code but no stderr output."
        elif not stdout:
            print("--- Logic filter không tạo ra output. Coi như lỗi. ---")
            error_message = "Logstash ran successfully but produced no output. The filter might have dropped the event."
        else:
            print("\n--- THÀNH CÔNG! Logic filter đã chính xác. ---")
            deploy_config_and_restart_logstash(current_code, final_config_path)
            return
            
    print(f"\n--- Thất bại sau {max_retries} lần thử. Không thể tạo và triển khai cấu hình. ---")

if __name__ == "__main__":
    if GOOGLE_API_KEY == 'YOUR_GOOGLE_API_KEY' or not GOOGLE_API_KEY:
        print("LỖI: Vui lòng thay thế 'YOUR_GOOGLE_API_KEY' bằng API Key của bạn trong file script.")
    else:
        main()
# -*- coding: utf-8 -*-
import time
import random
from kafka import KafkaProducer
import json

# --- CẤU HÌNH ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'raw-logs'
DELAY_SECONDS = 0.5  # Cứ 3 giây gửi một log mới

# --- DỮ LIỆU LOG MẪU ĐỂ GIẢ LẬP ---
# Danh sách log đã được cập nhật với các mẫu bạn cung cấp
SAMPLE_LOGS = [
    # Mẫu log Palo Alto mới được thêm
    "2025/07/24 14:34:06,013201036611,TRAFFIC,end,2562,2025/07/24 14:34:06,14.225.209.154,210.211.104.250,14.225.209.154,210.211.104.250,VPN_GP_in_VietNam_NuocNgoai,,,incomplete,vsys1,EDGE,EDGE,ethernet1/2,ethernet1/2,Forward_SysLog_127.11,2025/07/24 14:34:06,2260758,1,36523,80,36523,28869,0x400019,tcp,allow,152,78,74,2,2025/07/24 14:33:54,0,any,,7360121242230814409,0x8000000000000000,Viet Nam,Viet Nam,,1,1,aged-out,91,0,0,0,,PAN-IDC-5220-01,from-policy,,,0,,0,,N/A,0,0,0,0,db67a57a-1b36-4083-9e6b-948d88f03754,0,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2025-07-24T14:34:07.730+07:00,,,unknown,unknown,unknown,1,,,incomplete,no,no,0",
    "2025/07/24 14:34:06,013201036611,TRAFFIC,end,2562,2025/07/24 14:34:06,14.225.209.154,210.211.104.250,14.225.209.154,210.211.104.250,VPN_GP_in_VietNam_NuocNgoai,,,incomplete,vsys1,EDGE,EDGE,ethernet1/2,ethernet1/2,Forward_SysLog_127.11,2025/07/24 14:34:06,4138330,1,30082,80,30082,28869,0x400019,tcp,allow,152,78,74,2,2025/07/24 14:33:54,0,any,,7360121242230814410,0x8000000000000000,Viet Nam,Viet Nam,,1,1,aged-out,91,0,0,0,,PAN-IDC-5220-01,from-policy,,,0,,0,,N/A,0,0,0,0,db67a57a-1b36-4083-9e6b-948d88f03754,0,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2025-07-24T14:34:07.730+07:00,,,unknown,unknown,unknown,1,,,incomplete,no,no,0",
    "2025/07/24 14:34:06,013201036611,TRAFFIC,end,2562,2025/07/24 14:34:06,14.225.209.154,210.211.104.250,14.225.209.154,210.211.104.250,VPN_GP_in_VietNam_NuocNgoai,,,incomplete,vsys1,EDGE,EDGE,ethernet1/2,ethernet1/2,Forward_SysLog_127.11,2025/07/24 14:34:06,329839,1,57613,80,57613,28869,0x400019,tcp,allow,152,78,74,2,2025/07/24 14:33:54,0,any,,7360121242230814411,0x8000000000000000,Viet Nam,Viet Nam,,1,1,aged-out,91,0,0,0,,PAN-IDC-5220-01,from-policy,,,0,,0,,N/A,0,0,0,0,db67a57a-1b36-4083-9e6b-948d88f03754,0,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2025-07-24T14:34:07.730+07:00,,,unknown,unknown,unknown,1,,,incomplete,no,no,0",
    "2025/07/24 14:34:06,013201036611,TRAFFIC,end,2562,2025/07/24 14:34:06,172.28.104.84,192.168.7.11,0.0.0.0,0.0.0.0,ACCESS-TO-DNS-AD,,,dns-base,vsys1,SNP_BRANCHES,IDC-CORE,tunnel.463,ae1.304,Forward_SysLog_127.11,2025/07/24 14:34:06,4080506,1,56710,53,0,0,0x19,udp,allow,481,296,185,3,2025/07/24 14:33:34,0,any,,7360121242230814413,0x8000000000000000,KVTC,192.168.0.0-10.255.255.255,,2,1,aged-out,91,0,0,0,,PAN-IDC-5220-01,from-policy,,,0,,0,,N/A,0,0,0,0,146582ab-8966-4601-a0e8-6ce122cdcda0,0,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2025-07-24T14:34:07.730+07:00,,,infrastructure,networking,network-protocol,3,\"used-by-malware,has-known-vulnerability,pervasive-use\",dns,dns-base,no,no,0",
    "2025/07/24 14:34:06,013201036611,TRAFFIC,end,2562,2025/07/24 14:34:06,10.1.251.96,10.1.253.35,0.0.0.0,0.0.0.0,Access_F5_to_IDC-SRV-UAT-A01U-EOF-WEB-10.1.253.35,,,ssl,vsys1,IDC-TEST,IDC-TEST,ae1.704,ae1.702,Forward_SysLog_127.11,2025/07/24 14:34:06,3008170,1,36824,443,0,0,0x41c,tcp,allow,2850,1194,1656,15,2025/07/24 14:33:49,0,not-resolved,,7360121242230814412,0x8000000000000000,192.168.0.0-10.255.255.255,192.168.0.0-10.255.255.255,,8,7,tcp-fin,91,0,0,0,,PAN-IDC-5220-01,from-policy,,,0,,0,,N/A,0,0,0,0,441106d6-b454-4f89-a925-243302e04581,0,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2025-07-24T14:34:07.730+07:00,,,encrypted-tunnel,networking,browser-based,4,\"used-by-malware,able-to-transfer-file,has-known-vulnerability,tunnel-other-application,pervasive-use\",,ssl,no,no,0",
    "2025/07/24 14:34:06,013201036611,TRAFFIC,end,2562,2025/07/24 14:34:06,10.1.253.153,193.169.100.55,0.0.0.0,0.0.0.0,Temp_to_SNP-SRV-AD-TEST-193.169.100.55,,,dns-base,vsys1,IDC-TEST,IDC-CORE,ae1.702,ae1.304,Forward_SysLog_127.11,2025/07/24 14:34:06,841868,1,57362,53,0,0,0x19,udp,allow,324,122,202,2,2025/07/24 14:33:34,0,any,,7360121242230814414,0x8000000000000000,192.168.0.0-10.255.255.255,193.169.0.0-172.31.255.255,,1,1,aged-out,91,0,0,0,,PAN-IDC-5220-01,from-policy,,,0,,0,,N/A,0,0,0,0,4a96437b-45e2-4631-89d6-c0e9f56bba71,0,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2025-07-24T14:34:07.730+07:00,,,infrastructure,networking,network-protocol,3,\"used-by-malware,has-known-vulnerability,pervasive-use\",dns,dns-base,no,no,0",
    "2025/07/24 14:34:06,013201036611,TRAFFIC,end,2562,2025/07/24 14:34:06,14.225.209.154,210.211.104.122,0.0.0.0,0.0.0.0,Access_IDC_Website_TCT_210.211.104.122-123,,,incomplete,vsys1,EDGE,IDC-DMZ-PUBLIC,ethernet1/3,ae1.309,Forward_SysLog_127.11,2025/07/24 14:34:06,26077,1,8042,80,0,0,0x19,tcp,allow,312,78,234,4,2025/07/24 14:33:45,9,any,,7360121242230814434,0x8000000000000000,Viet Nam,Viet Nam,,1,3,aged-out,91,0,0,0,,PAN-IDC-5220-01,from-policy,,,0,,0,,N/A,0,0,0,0,f3eb56c6-0461-4911-a660-cb3a291004f2,0,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2025-07-24T14:34:07.730+07:00,,,unknown,unknown,unknown,1,,,incomplete,no,no,0"
]

def create_producer():
    """Tạo một Kafka producer."""
    print("Đang kết nối đến Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: v.encode('utf-8')
        )
        print("✅ Kết nối Kafka thành công!")
        return producer
    except Exception as e:
        print(f"❌ Lỗi! Không thể kết nối đến Kafka: {e}")
        return None

def main():
    """Gửi log mẫu đến Kafka một cách liên tục."""
    producer = create_producer()
    if not producer:
        print("Thoát chương trình do không thể kết nối Kafka.")
        return

    print(f"🚀 Bắt đầu gửi log giả lập đến topic '{KAFKA_TOPIC}' mỗi {DELAY_SECONDS} giây.")
    print("Nhấn Ctrl+C để dừng.")
    
    try:
        while True:
            log_message = random.choice(SAMPLE_LOGS)
            
            log_type = "Unknown"
            if "TRAFFIC" in log_message: log_type = "Palo Alto"
            elif "event_type" in log_message: log_type = "Suricata"
            elif "Check Point" in log_message: log_type = "Check Point"

            print(f"\n- - - - - - - - - - - - - - -")
            print(f"📨 Chuẩn bị gửi log loại: {log_type}")
            print(f"   Nội dung: {log_message[:120]}...")
            
            producer.send(KAFKA_TOPIC, log_message)
            producer.flush() 
            
            print(f"✅ Đã gửi thành công!")
            
            time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n🛑 Đã nhận lệnh dừng. Đang tắt producer...")
    finally:
        if producer:
            producer.close()
            print("Đã đóng kết nối Kafka.")

if __name__ == "__main__":
    main()
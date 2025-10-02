# parser.py
import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch
from google.generativeai import GenerativeModel, configure

# --- Cấu hình ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

# Cấu hình API Key từ file .env
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logging.error("Lỗi: Vui lòng cung cấp GEMINI_API_KEY trong file .env")
    exit(1)
configure(api_key=GEMINI_API_KEY)

# Cấu hình Kafka và Elasticsearch
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-logs")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ELASTICSEARCH_HOSTS = os.getenv("ELASTICSEARCH_HOSTS", "http://10.81.89.131:9200")

# ==============================================================================
LOG_PARSER_PROMPT = """
Bạn là một cỗ máy bóc tách log (Log Parser) cực kỳ chính xác. Nhiệm vụ của bạn là nhận một dòng log thô định dạng CSV từ tường lửa Palo Alto và chuyển đổi nó thành một đối tượng JSON có cấu trúc.

---
## HƯỚNG DẪN

1.  Dòng log đầu vào là một chuỗi CSV. Các giá trị được xếp theo thứ tự của danh sách "Header Fields" dưới đây.
2.  Nhiệm vụ của bạn là ghép đúng giá trị với đúng tên trường.
3.  Chuyển đổi các giá trị số (Bytes, Packets, Port) sang kiểu integer.
4.  Chỉ lấy tương ứng các trường và đổi tên lại các trường đó giống trong ví dụ mẫu
5.  Chỉ trả về **DUY NHẤT** một đối tượng JSON, không có bất kỳ văn bản giải thích hay ký tự ```json nào.

---
## DANH SÁCH TRƯỜNG (Header Fields) THEO ĐÚNG THỨ TỰ:
{header_fields}

---
## VÍ DỤ MẪU (Few-Shot Example)

**Log thô đầu vào (CSV):**
`2025/07/24 14:34:06,013201036611,TRAFFIC,end,2562,2025/07/24 14:34:06,14.225.209.154,210.211.104.250,14.225.209.154,210.211.104.250,VPN_GP_in_VietNam_NuocNgoai,,,incomplete,vsys1,EDGE,EDGE,ethernet1/2,ethernet1/2,Forward_SysLog_127.11,2260758,1,36523,80,36523,28869,0x400019,tcp,allow,152,78,74,2,2025/07/24 14:33:54,0,any,,7360121242230814409,0x8000000000000000,Viet Nam,Viet Nam,,1,1,aged-out,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2025-07-24T14:34:07.730+07:00,,,unknown,unknown,unknown,1,,,incomplete,no,no,0`

**JSON đầu ra:**
```json
{{
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
}}
```

## YÊU CẦU THỰC TẾ
Bây giờ, hãy bóc tách dòng log thô dưới đây.

**Log thô đầu vào:**
{raw_log}
"""
# ==============================================================================


# --- Các hàm xử lý ---

def load_headers(file_path: str) -> list[str]:
    """Đọc danh sách các trường từ file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logging.error(f"Lỗi: Không tìm thấy file 'headers.txt'. Vui lòng tạo file này.")
        exit(1)

async def parse_log_with_ai(raw_log: str, headers_str: str) -> dict:
    """Sử dụng Gemini để parse một dòng log CSV thô."""
    if not LOG_PARSER_PROMPT.strip() or "{raw_log}" not in LOG_PARSER_PROMPT:
        logging.warning("Prompt chưa được định nghĩa hoặc thiếu placeholder. Bỏ qua việc parse.")
        return None

    model = GenerativeModel('gemini-1.5-flash')
    prompt = LOG_PARSER_PROMPT.format(header_fields=headers_str, raw_log=raw_log)
    try:
        logging.info(f"Đang gửi log để parse: {raw_log[:120]}...")
        response = await model.generate_content_async(prompt)
        # Regex để trích xuất nội dung JSON một cách linh hoạt
        match = re.search(r'```json\s*([\s\S]*?)\s*```|({[\s\S]*})', response.text)
        if match:
            # Ưu tiên lấy từ block code ```json, nếu không có thì lấy đối tượng JSON đầu tiên
            json_str = match.group(1) or match.group(2)
            return json.loads(json_str)
        else:
            logging.warning(f"Không tìm thấy nội dung JSON hợp lệ trong phản hồi của AI.")
            logging.debug(f"Phản hồi từ AI: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Lỗi khi gọi API hoặc parse JSON: {e}")
        logging.debug(f"Phản hồi lỗi từ AI: {response.text if 'response' in locals() else 'N/A'}")
        return None

async def create_es_client() -> AsyncElasticsearch:
    """Tạo và kiểm tra kết nối đến Elasticsearch."""
    logging.info(f"Đang kết nối đến Elasticsearch tại {ELASTICSEARCH_HOSTS}...")
    es_client = AsyncElasticsearch(hosts=[ELASTICSEARCH_HOSTS])
    try:
        if await es_client.ping():
            logging.info("✅ Kết nối Elasticsearch thành công!")
            return es_client
        else:
            logging.error("❌ Kết nối Elasticsearch thất bại, server không phản hồi.")
            return None
    except Exception as e:
        logging.error(f"❌ Lỗi khi kết nối đến Elasticsearch: {e}")
        return None

async def send_to_elk(es_client: AsyncElasticsearch, document: dict):
    """Gửi một document đã được parse vào Elasticsearch."""
    index_name = f"logs-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"
    document['@timestamp'] = datetime.now(timezone.utc).isoformat()
    try:
        response = await es_client.index(index=index_name, document=document)
        logging.info(f"✅ Đã đẩy log vào index '{index_name}' thành công, id: {response['_id']}")
    except Exception as e:
        logging.error(f"❌ Lỗi khi đẩy log vào Elasticsearch: {e}")

async def main():
    """Chương trình chính: consume, parse, và đẩy log."""
    es_client = await create_es_client()
    if not es_client:
        return

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest'
    )
    await consumer.start()
    logging.info(f"✅ Đã kết nối Kafka, đang lắng nghe topic '{KAFKA_TOPIC}'...")

    headers = load_headers('headers.txt')
    headers_for_prompt = "\n".join(f"- {header}" for header in headers)

    try:
        async for msg in consumer:
            raw_log_string = msg.value.decode('utf-8')
            parsed_log = await parse_log_with_ai(raw_log_string, headers_for_prompt)

            if parsed_log:
                logging.info("✅ Log đã được Parse thành công.")
                await send_to_elk(es_client, parsed_log)
            else:
                logging.warning(f"❌ Parse thất bại cho log: {raw_log_string[:150]}...")

    finally:
        await consumer.stop()
        logging.info("Đã đóng kết nối Kafka.")
        if es_client:
            await es_client.close()
            logging.info("Đã đóng kết nối Elasticsearch.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Đã nhận lệnh dừng từ người dùng.")


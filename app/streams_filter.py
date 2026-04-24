import json
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = ['kafka:9092']
INPUT_TOPIC       = 'payments'
OUTPUT_TOPIC      = 'high-amount-alerts'
THRESHOLD         = 100000  # 10만원

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id='streams-filter-group',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

print(f"[Streams Filter] {THRESHOLD:,}원 이상 고액 거래 탐지 시작...")

for message in consumer:
    payment = message.value

    if payment['amount'] >= THRESHOLD:
        alert = {
            **payment,
            "alert": f"🚨 고액 거래 감지! {payment['amount']:,}원"
        }
        producer.send(OUTPUT_TOPIC, key=payment['user_id'].encode('utf-8'), value=alert)
        print(f"[ALERT] {alert['ts']} | {alert['user_id']} | {alert['amount']:,}원 → {OUTPUT_TOPIC}")
    else:
        print(f"[PASS]  {payment['ts']} | {payment['user_id']} | {payment['amount']:,}원 — 정상")
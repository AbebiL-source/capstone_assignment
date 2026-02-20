import json
import re
import csv
from datetime import datetime

INPUT_FILE = 'raw_data.jsonl'         
OUTPUT_CSV = 'clean_transactions.csv'  
OUTPUT_JSONL = 'clean_transactions.jsonl'  

def normalize_amount(raw_amount, currency='USD'):
    """Convert various amount formats to float in USD (assuming no real FX conversion needed)"""
    if raw_amount is None:
        return None
    
    amt_str = str(raw_amount).strip()
    
    cleaned = re.sub(r'[\$\s,€£¥]', '', amt_str)
    
    try:
        if '.' not in cleaned and len(cleaned) > 2:
            return float(cleaned) / 100.0
        return float(cleaned)
    except (ValueError, TypeError):
        return None

def is_test_record(record):
    """Heuristic to detect test/sandbox records - improve based on real data"""
    if 'flags' in record and isinstance(record['flags'], list):
        if any(f.lower() in ['test', 'sandbox'] for f in record['flags']):
            return True
    
    if 'customer' in record and 'email' in record['customer']:
        email = record['customer']['email'].lower()
        if '@example.com' in email or '@test.com' in email or 'sandbox' in email:
            return True
    
    if 'is_test' in record or 'test_mode' in record:
        val = record.get('is_test') or record.get('test_mode')
        if val in [True, 1, 'true', 'True']:
            return True
    
    return False

def extract_relevant_fields(line):
    try:
        data = json.loads(line.strip())
    except json.JSONDecodeError:
        return None

    event = data.get('event', {})
    entity = data.get('entity', {})
    payload = data.get('payload', data)  

    order_id = (
        entity.get('order', {}).get('id') or
        payload.get('order_id') or
        payload.get('orderId') or
        None
    )

    payment_id = (
        entity.get('payment', {}).get('id') or
        payload.get('id') or
        None
    )

    amount_raw = (
        payload.get('Amount') or
        payload.get('amount') or
        payload.get('amount_cents') or
        payload.get('value') or
        None
    )

    currency = payload.get('currency') or 'USD'

    status = payload.get('status') or event.get('type') or 'UNKNOWN'

    timestamp = event.get('ts') or payload.get('timestamp') or None

    amount_usd = normalize_amount(amount_raw, currency)

    is_test = is_test_record(data) or is_test_record(payload) or is_test_record(event)

    if amount_usd is None or order_id is None and payment_id is None:
        return None  

    if is_test:
        return None  

    return {
        'order_id': order_id,
        'payment_id': payment_id,
        'amount_usd': amount_usd,
        'currency': currency,
        'status': status,
        'timestamp': timestamp,
        'raw_event_type': event.get('type'),
        'source': event.get('source')
    }

clean_records = []
skipped = 0

print("Processing raw_data.jsonl...")

with open(INPUT_FILE, 'r', encoding='utf-8') as f:
    for i, line in enumerate(f, 1):
        if not line.strip():
            continue
        cleaned = extract_relevant_fields(line)
        if cleaned:
            clean_records.append(cleaned)
        else:
            skipped += 1
        if i % 5000 == 0:
            print(f"Processed {i} lines | Clean: {len(clean_records)} | Skipped: {skipped}")

print(f"\nDone! Clean records: {len(clean_records)} | Skipped: {skipped}")

if clean_records:
    keys = clean_records[0].keys()
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as out:
        writer = csv.DictWriter(out, fieldnames=keys)
        writer.writeheader()
        writer.writerows(clean_records)
    print(f"Saved to {OUTPUT_CSV}")

    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as out:
        for rec in clean_records:
            out.write(json.dumps(rec) + '\n')
    print(f"Also saved to {OUTPUT_JSONL}")
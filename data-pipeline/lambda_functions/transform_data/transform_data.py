import boto3
import json
import csv
import os

s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")


def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    if "bucket" in event and "key" in event:
        bucket = event["bucket"]
        key = event["key"]
    elif "detail" in event and "bucket" in event["detail"]:
        bucket = event["detail"]["bucket"]["name"]
        key = event["detail"]["object"]["key"]
    else:
        raise ValueError("Invalid event format: missing 'bucket' and 'key' fields")

    filename = key.split("/")[-1]
    print(f"Transforming file: {filename} (Bucket: {bucket})")

    # Handle only order files
    if filename.startswith("customer_orders_") and filename.endswith(".jsonl"):
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_data = obj["Body"].read().decode("utf-8").strip()

        orders = []
        for line in raw_data.splitlines():
            if line.strip():
                try:
                    orders.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON line: {e}")

        print(f"Loaded {len(orders)} orders for enrichment")

        
        proc_bucket = bucket.replace("input", "processing")
        cust_obj = s3.get_object(Bucket=proc_bucket, Key="customers.json")
        customers = json.loads(cust_obj["Body"].read().decode("utf-8"))
        customer_map = {c["customer_id"]: c["name"] for c in customers}

        prod_obj = s3.get_object(Bucket=proc_bucket, Key="product_catalog.csv")
        product_lines = prod_obj["Body"].read().decode("utf-8").splitlines()
        product_catalog = {r["product_id"]: r for r in csv.DictReader(product_lines)}

        table_name = "customer-data-pipeline-metadata-db"
        for order in orders:
            cust_name = customer_map.get(order.get("customer_id"), "Unknown")
            prod_info = product_catalog.get(order.get("product_id"), {})
            dynamodb.put_item(
                TableName=table_name,
                Item={
                    "record_id": {"S": order.get("order_id", "unknown")},
                    "customer_name": {"S": cust_name},
                    "product_name": {"S": prod_info.get("name", "Unknown")},
                    "price": {"S": prod_info.get("price", "0.00")},
                    "status": {"S": "processed"},
                },
            )

        print(f"Successfully wrote {len(orders)} items to {table_name}")
        return {"status": "transformed", "records": len(orders)}

    print(f"Skipped non-order file: {filename}")
    return {"status": "skipped", "filename": filename}

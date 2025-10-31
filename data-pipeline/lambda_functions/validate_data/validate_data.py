import boto3
import json
import csv
import os

s3 = boto3.client("s3")


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
    print(f"File detected: {filename} (Bucket: {bucket})")

    if filename in ["customers.json", "product_catalog.csv"]:
        processing_bucket = bucket.replace("input", "processing")
        print(f"Detected reference file. Moving to {processing_bucket}")
        copy_source = {"Bucket": bucket, "Key": key}
        s3.copy_object(CopySource=copy_source, Bucket=processing_bucket, Key=filename)
        return {"status": "reference_file_moved", "filename": filename}

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

        print(f"Read {len(orders)} order records from {filename}")

        proc_bucket = bucket.replace("input", "processing")
        cust_obj = s3.get_object(Bucket=proc_bucket, Key="customers.json")
        customers = json.loads(cust_obj["Body"].read().decode("utf-8"))
        valid_customer_ids = {c["customer_id"] for c in customers}

        prod_obj = s3.get_object(Bucket=proc_bucket, Key="product_catalog.csv")
        product_lines = prod_obj["Body"].read().decode("utf-8").splitlines()
        product_ids = {r["product_id"] for r in csv.DictReader(product_lines)}

        errors = []
        for order in orders:
            if "customer_id" not in order or "product_id" not in order:
                errors.append(f"Missing required fields in {order}")
                continue
            if order["customer_id"] not in valid_customer_ids:
                errors.append(f"Invalid customer_id: {order['customer_id']}")
            if order["product_id"] not in product_ids:
                errors.append(f"Invalid product_id: {order['product_id']}")

        if errors:
            raise Exception(
                f"Validation failed for {filename} with {len(errors)} errors"
            )

        print(f"Validation successful for {len(orders)} records in {filename}")
        return {"status": "validated", "records": len(orders)}

    print(f"Unknown file type: {filename}")
    return {"status": "skipped", "filename": filename}

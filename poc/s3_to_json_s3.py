import io
import os
import sys
import json
import zipfile
import boto3
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
        sys.argv,
        ["s3-url"])

client = boto3.client("s3")

def get_s3_url_components(s3_key):
    parse_result = urlparse(s3_key)
    components = {"bucket": parse_result.netloc,
                  "key": parse_result.path.lstrip("/")}
    return(components)


s3_url_components = get_s3_url_components(args["s3_url"])
s3_obj = client.get_object(
        Bucket = s3_url_components["bucket"],
        Key = s3_url_components["key"])
s3_obj_metadata = s3_obj["Metadata"]
with zipfile.ZipFile(io.BytesIO(s3_obj["Body"].read())) as z:
    contents = z.namelist()
    for json_path in z.namelist():
        dataset_name = os.path.splitext(json_path)[0]
        with z.open(json_path, "r") as p:
            j = json.load(p)
            if dataset_name == "metadata":
                for key in s3_obj_metadata:
                    j[key] = s3_obj_metadata[key]
            else:
                if type(j) == list:
                    for item in j:
                        item["recordid"] = s3_obj_metadata["recordid"]
                else:
                    j["recordid"] = s3_obj_metadata["recordid"]
            output_fname = s3_obj_metadata["recordid"] + ".ndjson"
            output_path = os.path.join(dataset_name, output_fname)
            os.makedirs(dataset_name, exist_ok=True)
            with open(output_path, "w") as f_out:
                json.dump(j, f_out, indent=None)
            s3_output_bucket = s3_url_components["bucket"]
            s3_output_prefix = "raw_json"
            s3_output_key = os.path.join(
                    s3_output_prefix, dataset_name, output_fname)
            with open(output_path, "rb") as f_in:
                response = client.put_object(
                        Body = f_in,
                        Bucket = s3_output_bucket,
                        Key = s3_output_key,
                        Metadata = s3_obj_metadata)



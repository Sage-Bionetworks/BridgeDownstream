import os
import io
import json
import zipfile
import boto3
import pytest
from src.glue.jobs import s3_to_json_s3
# requires pytest-datadir to be installed


class MockSynapse:

    def get_sts_storage_token(*args, **kwargs):
        sts_storage_token = {
                "bucket": None, "baseKey": None, "accessKeyId": None,
                "secretAccessKey": None, "sessionToken": None, "expiration": None
        }
        return sts_storage_token

class MockAWSClient:

    def put_object(*args, **kwargs):
        return None

class TestS3ToJsonS3():

    @pytest.fixture(scope="class")
    def schema_mapping_uri(self, artifact_bucket, namespace):
        schema_mapping_uri = (
                f"s3://{artifact_bucket}/BridgeDownstream/{namespace}/"
                "glue/resources/schema_mapping.json"
        )
        return schema_mapping_uri

    @pytest.fixture(scope="class")
    def dataset_mapping_uri(self, artifact_bucket, namespace):
        dataset_mapping_uri = (
                f"s3://{artifact_bucket}/BridgeDownstream/{namespace}/"
                "glue/resources/dataset_mapping.json"
        )
        return dataset_mapping_uri

    @pytest.fixture(scope="class")
    def dataset_mapping(self):
        dataset_mapping_local_path = "src/glue/resources/dataset_mapping.json"
        with open(dataset_mapping_local_path, "r") as f:
            dataset_mapping = json.load(f)
        return dataset_mapping

    @pytest.fixture(scope="class")
    def schema_mapping(self):
        schema_mapping_local_path = "src/glue/resources/schema_mapping.json"
        with open(schema_mapping_local_path, "r") as f:
            schema_mapping = json.load(f)
        return schema_mapping

    @pytest.fixture(scope="class")
    def archive_map(self):
        archive_map = {
          "anyOf": [
              {
                "filename": "metadata.json",
                "isRequired": True,
                "jsonSchema": "https://sage-bionetworks.github.io/mobile-client-json/schemas/v2/ArchiveMetadata.json"
              }
            ],
          "assessments" : [
            {
              "assessmentIdentifier" : "spelling",
              "assessmentRevision": 5,
              "files": [
                {
                  "filename": "taskData.json",
                  "contentType": "application/json",
                  "isRequired": True,
                  "jsonSchema": "https://raw.githubusercontent.com/MobileToolbox/MTBfx/937cdd1bf3b09815e97b53632c58208a14255b34/JSONschema/taskData_combinedSchema.json"
                }
              ]
            }
          ],
          "apps": [
            {
              "appId": "mobile-toolbox",
              "iOS": 0,
              "android" : 0,
              "default" : [
                  {
                      "organization": "Sage Bionetworks",
                      "files": [
                        {
                          "filename": "assessmentResult.json",
                          "isRequired": True,
                          "jsonSchema": "https://sage-bionetworks.github.io/mobile-client-json/schemas/v2/AssessmentResultObject.json"
                        }
                      ]
                  }
              ],
              "anyOf": [
                {
                  "filename": "motion.json",
                  "isRequired": False,
                  "jsonSchema": "https://sage-bionetworks.github.io/mobile-client-json/schemas/v2/MotionRecord.json"
                }
              ]
            }
          ]
        }
        return archive_map

    @pytest.fixture
    def s3_obj(self, shared_datadir):
        s3_obj = {
          "ResponseMetadata": {
            "RequestId": "4DMXBV2T5N6TCSJD",
            "HostId": "AXqkB5pklgH87t3jcyV7gTqFzvmHppe0x/JiNcGU4RXoyN++k/k5soqYoScSMW3ZpEvf5mF2Vlw=",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
              "x-amz-id-2": "AXqkB5pklgH87t3jcyV7gTqFzvmHppe0x/JiNcGU4RXoyN++k/k5soqYoScSMW3ZpEvf5mF2Vlw=",
              "x-amz-request-id": "4DMXBV2T5N6TCSJD",
              "date": "Mon, 19 Sep 2022 22:02:30 GMT",
              "last-modified": "Tue, 15 Feb 2022 18:48:17 GMT",
              "etag": "\"2f23f7dca97734a9910816af8ee01ae7\"",
              "x-amz-server-side-encryption": "AES256",
              "x-amz-meta-recordid": "OCJByUtSrVTYtqObYp7XZV_J",
              "x-amz-meta-schedulepublished": "true",
              "x-amz-meta-sessionguid": "aZJfKCutJD42m-Ly9jx9tY1R",
              "x-amz-meta-studyburstid": "timeline_retrieved_burst",
              "x-amz-meta-assessmentid": "spelling",
              "x-amz-meta-healthcode": "8d6L6xdo7u8jlszxR-ZkHW_q",
              "x-amz-meta-eventtimestamp": "2022-02-15T18:43:53.419Z",
              "x-amz-meta-sessioninstancestartday": "0",
              "x-amz-meta-sessioninstanceendday": "0",
              "x-amz-meta-sessionstarteventid": "study_burst:timeline_retrieved_burst:01",
              "x-amz-meta-assessmentrevision": "5",
              "x-amz-meta-studyburstnum": "1",
              "x-amz-meta-participantversion": "2",
              "x-amz-meta-uploadedon": "2022-02-15T18:48:15.890Z",
              "x-amz-meta-assessmentguid": "v0a2lR0umI_-EypGK2R5bX49",
              "x-amz-meta-sessioninstanceguid": "DcS_i05tJp1s-7cRDn0tCw",
              "x-amz-meta-assessmentinstanceguid": "_WmIJDzE7lUIoa8wSe2EOg",
              "x-amz-meta-timewindowguid": "OS9FFfgEKs1rlFzRYrYSVA58",
              "x-amz-meta-clientinfo": "ClientInfo [appName=MobileToolboxApp, appVersion=73, deviceName=iPhone 7 Plus, osName=iPhone OS, osVersion=15.1, sdkName=BridgeClientKMM, sdkVersion=0]",
              "x-amz-meta-instanceguid": "_WmIJDzE7lUIoa8wSe2EOg",
              "x-amz-meta-scheduleguid": "gVBGLqW_IcQbhAQ4DdR6nD3_",
              "x-amz-meta-schedulemodifiedon": "2022-02-09T23:45:44.010Z",
              "x-amz-meta-exportedon": "2022-02-15T18:48:16.167Z",
              "accept-ranges": "bytes",
              "content-type": "application/zip",
              "server": "AmazonS3",
              "content-length": "279963"
            },
            "RetryAttempts": 0
          },
          "AcceptRanges": "bytes",
          "LastModified": "2022-02-15 18:48:17.000",
          "ContentLength": 279963,
          "ETag": "\"2f23f7dca97734a9910816af8ee01ae7\"",
          "ContentType": "application/zip",
          "ServerSideEncryption": "AES256",
          "Metadata": {
            "recordid": "OCJByUtSrVTYtqObYp7XZV_J",
            "schedulepublished": "true",
            "sessionguid": "aZJfKCutJD42m-Ly9jx9tY1R",
            "studyburstid": "timeline_retrieved_burst",
            "assessmentid": "spelling",
            "healthcode": "8d6L6xdo7u8jlszxR-ZkHW_q",
            "eventtimestamp": "2022-02-15T18:43:53.419Z",
            "sessioninstancestartday": "0",
            "sessioninstanceendday": "0",
            "sessionstarteventid": "study_burst:timeline_retrieved_burst:01",
            "assessmentrevision": "5",
            "studyburstnum": "1",
            "participantversion": "2",
            "uploadedon": "2022-02-15T18:48:15.890Z",
            "assessmentguid": "v0a2lR0umI_-EypGK2R5bX49",
            "sessioninstanceguid": "DcS_i05tJp1s-7cRDn0tCw",
            "assessmentinstanceguid": "_WmIJDzE7lUIoa8wSe2EOg",
            "timewindowguid": "OS9FFfgEKs1rlFzRYrYSVA58",
            "clientinfo": "ClientInfo [appName=MobileToolboxApp, appVersion=73, deviceName=iPhone 7 Plus, osName=iPhone OS, osVersion=15.1, sdkName=BridgeClientKMM, sdkVersion=0]",
            "instanceguid": "_WmIJDzE7lUIoa8wSe2EOg",
            "scheduleguid": "gVBGLqW_IcQbhAQ4DdR6nD3_",
            "schedulemodifiedon": "2022-02-09T23:45:44.010Z",
            "exportedon": "2022-02-15T18:48:16.167Z"
          }
        }
        with open(shared_datadir / "OCJByUtSrVTYtqObYp7XZV_J-mtbSpelling.zip", "rb") as z:
            s3_obj["Body"] = z.read()
        return s3_obj

    @pytest.fixture(scope="class")
    def metadata_json_schema(self):
        metadata_json_schema = {
          "$id" : "https://sage-bionetworks.github.io/mobile-client-json/schemas/v2/ArchiveMetadata.json",
          "$schema" : "http://json-schema.org/draft-07/schema#",
          "type" : "object",
          "title" : "ArchiveMetadata",
          "description" : "The metadata for an archive that can be zipped using the app developer's choice of third-party archival tools.",
          "definitions" : {
            "FileInfo" : {
              "$id" : "#FileInfo",
              "type" : "object",
              "title" : "FileInfo",
              "description" : "",
              "properties" : {
                "filename" : {
                  "type" : "string",
                  "description" : "The filename of the archive object. This should be unique within the manifest.",
                  "format" : "uri-relative"
                },
                "timestamp" : {
                  "type" : "string",
                  "description" : "The file creation date.",
                  "format" : "date-time"
                },
                "contentType" : {
                  "type" : "string",
                  "description" : "The content type of the file."
                },
                "identifier" : {
                  "type" : "string",
                  "description" : "The identifier for the result."
                },
                "stepPath" : {
                  "type" : "string",
                  "description" : "The full path to the result if it is within the step history."
                },
                "jsonSchema" : {
                  "type" : "string",
                  "description" : "The uri for the json schema if the content type is 'application/json'.",
                  "format" : "uri"
                },
                "metadata" : {
                  "description" : "Any additional metadata about this file."
                }
              },
              "required" : [
                "filename",
                "timestamp"
              ],
              "additionalProperties" : False,
              "examples" : [
                {
                  "filename" : "foo.json",
                  "timestamp" : "2022-06-14T11:29:59.915-07:00",
                  "contentType" : "application/json",
                  "identifier" : "foo",
                  "stepPath" : "Bar/foo",
                  "jsonSchema" : "http://example.org/schemas/v1/Foo.json",
                  "metadata" : {
                    "value" : 1
                  }
                }
              ]
            }
          },
          "properties" : {
            "appName" : {
              "type" : "string",
              "description" : "Name of the app that built the archive."
            },
            "appVersion" : {
              "type" : "string",
              "description" : "Version of the app that built the archive."
            },
            "deviceInfo" : {
              "type" : "string",
              "description" : "Information about the specific device."
            },
            "deviceTypeIdentifier" : {
              "type" : "string",
              "description" : "Specific model identifier of the device."
            },
            "files" : {
              "type" : "array",
              "description" : "A list of the files included in this archive.",
              "items" : {
                "$ref" : "#/definitions/FileInfo"
              }
            }
          },
          "required" : [
            "appName",
            "appVersion",
            "deviceInfo",
            "deviceTypeIdentifier",
            "files"
          ]
        }
        return metadata_json_schema

    def test_get_dataset_mapping(self, dataset_mapping_uri, dataset_mapping):
        remote_dataset_mapping = s3_to_json_s3.get_data_mapping(data_mapping_uri=dataset_mapping_uri)
        assert remote_dataset_mapping == dataset_mapping

    def test_get_schema_mapping(self, schema_mapping_uri, schema_mapping):
        remote_schema_mapping = s3_to_json_s3.get_data_mapping(data_mapping_uri=schema_mapping_uri)
        assert remote_schema_mapping == schema_mapping

    def test_get_archive_map(self, artifact_bucket, namespace):
        archive_map = s3_to_json_s3.get_archive_map(archive_map_version="v4.4.1")
        assert isinstance(archive_map, dict)

    def test_update_sts_tokens(self):
        synapse_data_folder_1 = "syn11111111"
        sts_tokens = s3_to_json_s3.update_sts_tokens(
                syn=MockSynapse(),
                synapse_data_folder=synapse_data_folder_1,
                sts_tokens={})
        assert synapse_data_folder_1 in sts_tokens

    def test_get_cached_json_schema(self):
        json_schema = {"url": "url", "schema": "schema", "app_id": None, "assessment_id": None,
                "assessment_revision": None, "file_name": None, "archive_map_version": None}
        cached_json_schema = s3_to_json_s3._get_cached_json_schema(
            url="url",
            json_schemas=[json_schema]
        )
        assert cached_json_schema == "schema"

    def test_get_json_schema_universal_file(self, archive_map):
        file_metadata_metadata = {
                "assessment_id": "spelling",
                "assessment_revision": 5,
                "file_name": "metadata.json",
                "app_id": "mobile-toolbox"
        }
        json_schema_metadata = s3_to_json_s3.get_json_schema(
                archive_map=archive_map,
                file_metadata=file_metadata_metadata,
                json_schemas={}
        )
        assert isinstance(json_schema_metadata, dict)
        assert isinstance(json_schema_metadata["schema"], dict)

    def test_get_json_schema_assessment_specific_file(self, archive_map):
        file_metadata_taskdata = {
                "assessment_id": "spelling",
                "assessment_revision": 5,
                "file_name": "taskData.json",
                "app_id": "mobile-toolbox"
        }
        json_schema_taskdata = s3_to_json_s3.get_json_schema(
                archive_map=archive_map,
                file_metadata=file_metadata_taskdata,
                json_schemas={}
        )
        assert isinstance(json_schema_taskdata, dict)
        assert isinstance(json_schema_taskdata["schema"], dict)

    def test_get_json_schema_app_specific_file(self, archive_map):
        file_metadata_motion = {
                "assessment_id": "spelling",
                "assessment_revision": 5,
                "file_name": "motion.json",
                "app_id": "mobile-toolbox"
        }
        json_schema_motion = s3_to_json_s3.get_json_schema(
                archive_map=archive_map,
                file_metadata=file_metadata_motion,
                json_schemas={}
        )
        assert isinstance(json_schema_motion, dict)
        assert isinstance(json_schema_motion["schema"], dict)

    def test_get_json_schema_app_specific_default_file(self, archive_map):
        file_metadata_assessment_result = {
                "assessment_id": "spelling",
                "assessment_revision": 5,
                "file_name": "assessmentResult.json",
                "app_id": "mobile-toolbox"
        }
        json_schema_assessment_result = s3_to_json_s3.get_json_schema(
                archive_map=archive_map,
                file_metadata=file_metadata_assessment_result,
                json_schemas={}
        )
        assert isinstance(json_schema_assessment_result, dict)
        assert isinstance(json_schema_assessment_result["schema"], dict)

    def test_get_json_schema_unlisted_filename(self, archive_map):
        file_metadata_no_schema = {
                "assessment_id": "spelling",
                "assessment_revision": 5,
                "file_name": "jellybeanz.json",
                "app_id": "mobile-toolbox"
        }
        json_schema_no_schema = s3_to_json_s3.get_json_schema(
                archive_map=archive_map,
                file_metadata=file_metadata_no_schema,
                json_schemas={}
        )
        assert isinstance(json_schema_no_schema, dict)
        assert json_schema_no_schema["schema"] is None

    def test_update_json_schemas(self, s3_obj, archive_map):
        json_schemas = s3_to_json_s3.update_json_schemas(
                s3_obj=s3_obj,
                archive_map=archive_map,
                json_schemas=[]
        )
        assert isinstance(json_schemas, list)
        file_names = [
                "taskData", "taskData.json", "info.json", "motion.json",
                "weather.json", "taskResult.json", "metadata.json"]
        actual_file_names = [j["file_name"] for j in json_schemas]
        for file_name in file_names:
            assert file_name in actual_file_names

    def test_get_dataset_identifier_has_schema(self, schema_mapping, dataset_mapping):
        json_schema = {
                "$id": ("https://sage-bionetworks.github.io/mobile-client-json/"
                        "schemas/v2/ArchiveMetadata.json")
        }
        schema_dataset_identifier = s3_to_json_s3.get_dataset_identifier(
                json_schema=json_schema,
                schema_mapping=schema_mapping,
                dataset_mapping=dataset_mapping,
                file_metadata={}
        )
        assert schema_dataset_identifier == "ArchiveMetadata_v1"

    def test_get_dataset_identifier_does_not_have_schema(self, schema_mapping, dataset_mapping):
        file_metadata = {
                "assessment_id": "dccs",
                "assessment_revision": "5",
                "file_name": "motion.json",
                "app_id": "mobile-toolbox"
        }
        data_dataset_identifier = s3_to_json_s3.get_dataset_identifier(
                json_schema={ "schema": None },
                schema_mapping=schema_mapping,
                dataset_mapping=dataset_mapping,
                file_metadata=file_metadata
        )
        assert data_dataset_identifier == "MotionRecord_v1"

    def test_get_dataset_identifier_unlisted_assessment_id(self, schema_mapping, dataset_mapping):
        file_metadata = {
                "assessment_id": "jellybeanz",
                "assessment_revision": "5",
                "file_name": "motion.json",
                "app_id": "mobile-toolbox"
        }
        data_dataset_identifier = s3_to_json_s3.get_dataset_identifier(
                json_schema={ "schema": None },
                schema_mapping=schema_mapping,
                dataset_mapping=dataset_mapping,
                file_metadata=file_metadata
        )
        assert data_dataset_identifier is None

    def test_get_dataset_identifier_unlisted_assessment_revision(self, schema_mapping, dataset_mapping):
        file_metadata = {
                "assessment_id": "dccs",
                "assessment_revision": "-1",
                "file_name": "motion.json",
                "app_id": "mobile-toolbox"
        }
        data_dataset_identifier = s3_to_json_s3.get_dataset_identifier(
                json_schema={ "schema": None },
                schema_mapping=schema_mapping,
                dataset_mapping=dataset_mapping,
                file_metadata=file_metadata
        )
        assert data_dataset_identifier is None

    def test_get_dataset_identifier_unlisted_file_name(self, schema_mapping, dataset_mapping):
        file_metadata = {
                "assessment_id": "dccs",
                "assessment_revision": "5",
                "file_name": "jellybeanz.json",
                "app_id": "mobile-toolbox"
        }
        data_dataset_identifier = s3_to_json_s3.get_dataset_identifier(
                json_schema={ "schema": None },
                schema_mapping=schema_mapping,
                dataset_mapping=dataset_mapping,
                file_metadata=file_metadata
        )
        assert data_dataset_identifier is None

    def test_validate_against_schema(self, s3_obj, metadata_json_schema):
        file_metadata = {
                "assessment_id": s3_obj["Metadata"]["assessmentid"],
                "assessment_revision": s3_obj["Metadata"]["assessmentrevision"],
                "file_name": "metadata.json",
                "app_id": "mobile-toolbox"
        }
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            with z.open(file_metadata["file_name"], "r") as p:
                j = json.load(p)
                all_errors = s3_to_json_s3.validate_against_schema(
                        data=j,
                        schema=metadata_json_schema
                )
                assert isinstance(all_errors, list)
                incorrect_metadata_json_schema = metadata_json_schema
                incorrect_metadata_json_schema["properties"]["cookies"] = { "type": "string" }
                incorrect_metadata_json_schema["required"] = \
                        incorrect_metadata_json_schema["required"] + ["cookies"]
                all_errors = s3_to_json_s3.validate_against_schema(
                        data=j,
                        schema=incorrect_metadata_json_schema
                )
                assert len(all_errors) == 1

    def test_validate_data_no_schemas(self, s3_obj, archive_map, dataset_mapping):
        s3_obj["Metadata"]["assessmentid"] = "dccs"
        s3_obj["Metadata"]["assessmentrevision"] = "5"
        validation_result = s3_to_json_s3.validate_data(
                s3_obj=s3_obj,
                archive_map=archive_map,
                json_schemas=[],
                dataset_mapping=dataset_mapping)
        assert isinstance(validation_result, dict)
        assert len(validation_result["errors"]) == 0
        required_keys = [
                "assessmentId", "assessmentRevision", "appId", "recordId", "errors"]
        for key in required_keys:
            assert key in validation_result
        assert isinstance(validation_result["errors"], dict)

    def test_validate_data(self, s3_obj, archive_map, dataset_mapping):
        validation_result = s3_to_json_s3.validate_data(
                s3_obj=s3_obj,
                archive_map=archive_map,
                json_schemas=[],
                dataset_mapping=dataset_mapping)
        assert isinstance(validation_result, dict)
        required_keys = [
                "assessmentId", "assessmentRevision", "appId", "recordId", "errors"]
        for key in required_keys:
            assert key in validation_result
        assert isinstance(validation_result["errors"], dict)

    def test_is_expected_validation_error(self):
        validation_result = {
                "assessmentId": "flanker",
                "assessmentRevision": "5",
                "appId": "mobile-toolbox",
                "recordId": "123456789",
                "errors": {}
        }
        client_info = "{osName:'Android'}"
        # test not validation_result["errors"]
        assert not s3_to_json_s3.is_expected_validation_error(
                validation_result=validation_result,
                client_info=client_info)
        # test "Android" not in client_info:
        assert not s3_to_json_s3.is_expected_validation_error(
                validation_result=validation_result,
                client_info="{osName:'iOS'}")
        # test validation_result["appId"] != "mobile-toolbox":
        assert not s3_to_json_s3.is_expected_validation_error(
            validation_result = {
                    "assessmentId": "flanker",
                    "assessmentRevision": "5",
                    "appId": "example-app",
                    "recordId": "123456789",
                    "errors": {}
            },
            client_info=client_info)
        # test metadata.json
        validation_result["errors"] = {
                "metadata.json": [
                    "'appName' is a required property",
                    "'files' is a required property"
                ]
        }
        assert s3_to_json_s3.is_expected_validation_error(
                validation_result=validation_result,
                client_info=client_info)
        # test taskData.json
        validation_result["errors"] = {
                "taskData.json": [
                    "Additional properties are not allowed ('type' was unexpected)"
                ]
        }
        assert s3_to_json_s3.is_expected_validation_error(
                validation_result=validation_result,
                client_info=client_info)
        # test weather.json
        validation_result["errors"] = {
                "weather.json": [
                    "'type' is a required property"
                ]
        }
        assert s3_to_json_s3.is_expected_validation_error(
                validation_result=validation_result,
                client_info=client_info)
        # test motion.json
        validation_result["errors"] = {
                "motion.json": [
                    "'type' is a required property"
                ]
        }
        assert s3_to_json_s3.is_expected_validation_error(
                validation_result=validation_result,
                client_info=client_info)

    def test_write_metadata_file_to_json_dataset(self, s3_obj, namespace, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x : MockAWSClient())
        workflow_run_properties = {
                "namespace": namespace,
                "app_name": "mobile-toolbox",
                "study_name": "dummy-study",
                "json_prefix": "raw-json",
                "json_bucket": "json-bucket"
        }
        partition_fields = ["assessmentid", "year", "month", "day", "recordid"]
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            output_file = s3_to_json_s3.write_file_to_json_dataset(
                    z = z,
                    json_path="metadata.json",
                    dataset_identifier="ArchiveMetadata_v1",
                    s3_obj_metadata=s3_obj["Metadata"],
                    workflow_run_properties=workflow_run_properties)
            with open(output_file, "r") as f_out:
                metadata = json.load(f_out)
                for metadata_key in s3_obj["Metadata"]:
                    assert metadata_key in metadata
                for partition_key in partition_fields:
                    assert partition_key in metadata

    def test_write_weather_file_to_json_dataset(self, s3_obj, namespace, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x : MockAWSClient())
        workflow_run_properties = {
                "namespace": namespace,
                "app_name": "mobile-toolbox",
                "study_name": "dummy-study",
                "json_prefix": "raw-json",
                "json_bucket": "json-bucket"
        }
        partition_fields = ["assessmentid", "year", "month", "day", "recordid"]
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            output_file = s3_to_json_s3.write_file_to_json_dataset(
                    z = z,
                    json_path="weather.json",
                    dataset_identifier="WeatherResult_v1",
                    s3_obj_metadata=s3_obj["Metadata"],
                    workflow_run_properties=workflow_run_properties)
            with open(output_file, "r") as f_out:
                weather = json.load(f_out)
                for partition_key in partition_fields:
                    assert partition_key in weather

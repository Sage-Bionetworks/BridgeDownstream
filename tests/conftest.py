import pytest
# requires pytest-datadir to be installed

def pytest_addoption(parser):
    parser.addoption("--namespace")
    parser.addoption("--artifact-bucket")

@pytest.fixture
def namespace(request):
    return request.config.getoption("namespace")

@pytest.fixture
def artifact_bucket(request):
    return request.config.getoption("artifact_bucket")

@pytest.fixture
def syn():
    pass

@pytest.fixture
def archive_map():
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
          "default" : {
              "files": [
                {
                  "filename": "assessmentResult.json",
                  "isRequired": True,
                  "jsonSchema": "https://sage-bionetworks.github.io/mobile-client-json/schemas/v2/AssessmentResultObject.json"
                }
              ]
          },
          "anyOf": [
            {
              "filename": "motion.json",
              "isRequired": False,
              "jsonSchema": "https://sage-bionetworks.github.io/mobile-client-json/schemas/v2/MotionRecord.json"
            }
          ],
          "assessments": [
            {
              "assessmentIdentifier": "spelling",
              "assessmentRevision": 5
            }
          ]
        }
      ]
    }
    return archive_map

@pytest.fixture
def s3_obj(shared_datadir):
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

#def safe_load_config(artifact_bucket, namespace):
#    config_uri = (
#            f"s3://{artifact_bucket}/BridgeDownstream/{namespace}/"
#            "config/config.yaml"
#    )
#    reque

FROM python:3.9.10-slim-bullseye

RUN apt-get update -y && apt-get upgrade -y && apt-get install git -y && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip install boto3==1.16.33 pandas==1.1.5 synapseclient==2.5.1 pyarrow==4.0.0
RUN git clone https://github.com/Sage-Bionetworks/BridgeDownstream.git /root/BridgeDownstream

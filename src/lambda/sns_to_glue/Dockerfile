FROM public.ecr.aws/lambda/python:3.8

RUN pip3 install synapseclient
COPY app.py .
CMD ["app.handler"]

FROM amazon/aws-glue-libs:glue_libs_3.0.0_image_01

RUN pip3 install synapseclient==2.5.* pyspark==3.* pyarrow==9.* pytest-datadir
ENTRYPOINT ["bash", "-l"]

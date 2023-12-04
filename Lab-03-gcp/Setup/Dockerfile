FROM python:3.11

WORKDIR /app
# Python scripts now live in etl_scripts
# so now I just copy the entire directory
COPY etl_scripts/* etl_scripts/

# switching to using requirements.txt instead of list of packages
RUN pip install -r etl_scripts/requirements.txt

ENTRYPOINT [ "python", "etl_scripts/pipeline.py" ]
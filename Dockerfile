From python:3.8.12-slim-buster
WORKDIR /home/code
COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN mkdir files
RUN mkdir result
COPY * ./

CMD ["python", "cvat_to_mysql.py"]

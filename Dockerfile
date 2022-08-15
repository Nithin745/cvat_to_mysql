From ubuntu:20.04
RUN apt-get update && \
	apt-get install -y gcc python3-dev libkrb5-dev \
	python3-pip build-essential \
	libgeos-dev \
        libldap2-dev \
        libsasl2-dev 
WORKDIR /home/code
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . .

CMD ["python", "cvat_to_mongogt.py"]

FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt .

RUN yum update -y && yum install -y gcc gcc-c++ protobuf-devel protobuf-compiler

RUN python3.9 -m pip install -r requirements.txt

COPY . .

CMD ["app.handle"]

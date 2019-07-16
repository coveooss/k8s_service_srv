FROM python:3-alpine

ADD requirements.txt /
ADD k8s_services_srv.py /

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "./k8s_services_srv.py" ]
CMD [ "" ]
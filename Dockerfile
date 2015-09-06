FROM python:2.7

RUN pip install	docker-py 				\
	elasticsearch					\
	pymongo 					\
	pyyaml

COPY ./es.py /bin/es.py

CMD ["/bin/es.py"]

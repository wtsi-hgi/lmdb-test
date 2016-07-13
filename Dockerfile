FROM python:2.7

WORKDIR /root
ADD . .
RUN pip install -r requirements.txt

CMD python ~/run.py
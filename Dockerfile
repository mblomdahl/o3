FROM python:3.6
LABEL maintainer="Mats Blomdahl <mats.blomdahl@gmail.com>"

ADD requirements.txt .

RUN pip install -r requirements.txt

CMD /usr/bin/python3

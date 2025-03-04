from python:3.12-bullseye

COPY ./requirements.txt /
RUN pip3 install -r requirements.txt

CMD ["bash"]

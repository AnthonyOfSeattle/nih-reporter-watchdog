from quay.io/jupyter/scipy-notebook:2025-03-05

COPY ./requirements.txt /
RUN pip3 install -r /requirements.txt

COPY ./jupyter_server_config.py ${HOME}/.jupyter/

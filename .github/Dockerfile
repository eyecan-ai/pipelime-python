ARG pyver
FROM python:${pyver}-bookworm

# graphviz
RUN apt-get update
RUN apt-get install -y graphviz graphviz-dev

# pip
RUN python -m pip install --no-cache-dir --upgrade pip
RUN python -m pip install --no-cache-dir --upgrade wheel flake8

# pipelime
COPY . /pipelime
WORKDIR /pipelime
RUN python -m pip install --upgrade -e ".[draw,tests]"

CMD ["pytest", ".", "-x"]

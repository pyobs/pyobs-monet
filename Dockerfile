FROM thusser/pyobs

# install pyobs-monet
COPY . /src
WORKDIR /src
RUN pip install -r requirements.txt
RUN python setup.py install

# clean up
RUN rm -rf /src

# set entrypoint
ENTRYPOINT ["/usr/local/bin/pyobs", "/pyobs.yaml"]
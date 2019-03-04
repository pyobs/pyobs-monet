FROM thusser/pytel

# install pytel-monet
COPY . /src
WORKDIR /src
RUN pip install -r requirements.txt
RUN python setup.py install

# clean up
RUN rm -rf /src

# set entrypoint
ENTRYPOINT ["bin/pytel", "/pytel.yaml"]
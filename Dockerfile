FROM ubuntu:18.04
RUN apt-get update -y \
    && apt-get upgrade -y \ 
    && apt-get install -y g++ make cmake automake libtool protobuf-compiler libprotoc-dev libgoogle-glog-dev pkg-config git
ADD . /SimpleDB
RUN cd /SimpleDB/libuv && make clean && ./autogen.sh && ./configure && make -j$(nproc)
RUN rm -rf /SimpleDB/leveldb/build
RUN mkdir /SimpleDB/leveldb/build && cd /SimpleDB/leveldb/build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
RUN cd /SimpleDB && make clean
RUN cd /SimpleDB/src && make
RUN cd /SimpleDB/baseline && make
RUN mkdir -p /tmp/simpledb/cache /tmp/__gg__/blobs /tmp/__gg__/reductions
ENV GG_DIR=/tmp/__gg__
ENV GG_EXEC_DIR=/tmp/simpledb/cache
EXPOSE 8080
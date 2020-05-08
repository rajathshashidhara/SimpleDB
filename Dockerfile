FROM ubuntu:18.04
RUN apt-get update -y \
    && apt-get upgrade -y \ 
    && apt-get install -y g++ make cmake automake libtool protobuf-compiler libprotoc-dev libgoogle-glog-dev pkg-config git
RUN mkdir -p /home/rajaths/SimpleDB && cd /home/rajaths && git clone --recurse-submodules https://github.com/rajathshashidhara/SimpleDB.git
RUN cd /home/rajaths/SimpleDB/libuv && ./autogen.sh && ./configure && make
RUN mkdir /home/rajaths/SimpleDB/leveldb/build && cd /home/rajaths/SimpleDB/leveldb/build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
RUN cd /home/rajaths/SimpleDB/src && make
RUN cd /home/rajaths/SimpleDB/baseline && make
RUN mkdir -p /tmp/simpledb/cache /tmp/__gg__/blobs /tmp/__gg__/reductions
ENV GG_DIR=/tmp/__gg__
ENV GG_EXEC_DIR=/tmp/simpledb/cache
EXPOSE 8080
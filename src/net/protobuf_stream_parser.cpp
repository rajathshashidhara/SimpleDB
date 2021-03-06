#include <iostream>
#include <glog/logging.h>
#include <climits>
#include <google/protobuf/io/coded_stream.h>
#include "protobuf_stream_parser.h"
#include "formats/netformats.pb.h"
#include "formats/execformats.pb.h"

using namespace std;

template <class Message>
bool ProtobufStreamParser<Message>::parsing_step()
{
    Message resp;
    string lenbuf;

    switch (state)
    {
    case READ_LEN_PENDING:
        if (buffer_.size() < sizeof(in_progress_message_len))
            return false;

        lenbuf = buffer_.get_and_pop_bytes(sizeof(in_progress_message_len));
        in_progress_message_len = *((size_t*) &lenbuf[0]);
        state = READ_PAYLOAD_PENDING;

        return true;

    case READ_PAYLOAD_PENDING:
    {
        if (buffer_.size() < in_progress_message_len)
            return false;

        string data = move(buffer_.get_and_pop_bytes(in_progress_message_len));
        google::protobuf::io::CodedInputStream istream((const uint8_t*) data.c_str(), data.length());
        istream.SetTotalBytesLimit(INT_MAX, INT_MAX);
        if (!resp.ParseFromCodedStream(&istream))
            throw runtime_error("Failed to parse Message");

        complete_messages_.emplace(move(resp));
        in_progress_message_len = 0;
        state = READ_LEN_PENDING;

        return true;
    }
    default:
        throw runtime_error("Invalid parse state");
    }
}

template <class Message>
void ProtobufStreamParser<Message>::parse(const string& buf)
{
    /* append buf to internal buffer */
    buffer_.append( buf );

    /* parse as much as we can */
    while ( parsing_step() ) {}
}

template class
ProtobufStreamParser<simpledb::proto::KVRequest>;
template class
ProtobufStreamParser<simpledb::proto::ExecResponse>;
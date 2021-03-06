#ifndef EXEC_RESPONSE_PARSER_HH
#define EXEC_RESPONSE_PARSER_HH

#include <string>
#include <queue>
#include <cassert>

#include <google/protobuf/message.h>

template <class Message>
class ProtobufStreamParser
{
private:
    class InternalBuffer
    {
    private:
        std::string buffer_ {};

    public:
        std::string get_and_pop_bytes(const size_t num)
        {
            assert(buffer_.length() >= num);

            std::string val(buffer_.substr(0, num));
            buffer_.replace(0, num, std::string());

            return val;
        }

        size_t size() { return buffer_.length(); }

        bool empty() const { return buffer_.empty(); }

        void append( const std::string & str ) { buffer_.append( str ); }

        const std::string & str() const { return buffer_; }
    };

    /* bytes that haven't been parsed yet */
    InternalBuffer buffer_ {};

    /* complete messages ready to go */
    std::queue<Message> complete_messages_ {};

    bool parsing_step();

protected:
    enum ProtobufStreamParseState {
        READ_LEN_PENDING,
        READ_PAYLOAD_PENDING
    };

    size_t in_progress_message_len;
    ProtobufStreamParseState state;

public:
    ProtobufStreamParser()
        : in_progress_message_len(0), state(READ_LEN_PENDING) {
            static_assert(std::is_base_of<google::protobuf::Message, Message>::value,
                "Invalid substitution in ProtobufStreamParser");
    }
    ~ProtobufStreamParser() {}

    /* must accept all of buf */
    void parse( const std::string & buf );

    /* getters */
    bool empty() const { return complete_messages_.empty(); }
    const Message & front() const
        { return complete_messages_.front(); }

    /* pop one request */
    void pop() { complete_messages_.pop(); }
};

#endif /* EXEC_RESPONSE_PARSER_HH */

#include "execution/runtime.h"
#include "util/exception.h"

using namespace std;

ExecutionState::~ExecutionState()
{
    static auto close_cb = [](uv_handle_t* handle) {};
    if (input_pipe != nullptr)
    {
        uv_close((uv_handle_t*) input_pipe, close_cb);
        delete input_pipe;
    }
    if (output_pipe != nullptr)
    {
        uv_close((uv_handle_t*) input_pipe, close_cb);
        delete output_pipe;
    }

    if (process != nullptr)
    {
        uv_close((uv_handle_t*) process, close_cb);
        delete process;
    }
}

void ExecutionState::Spawn(const ExecComplCallback& exec_cb)
{
    exec_compl_cb_ = exec_cb;

    int ret;
    char* argv[2];
    argv[0] = &function[0];
    argv[1] = NULL;

    input_pipe = new uv_pipe_t();
    output_pipe = new uv_pipe_t();

    if (input_pipe == nullptr || output_pipe == nullptr)
        throw runtime_error("Out of Memory!");


    ret = uv_pipe_init(client->execution_loop, input_pipe, 1);
    if (ret < 0)
        throw uv_error("Failed to initialize IPC pipe.", ret);
    ret = uv_pipe_init(client->execution_loop, output_pipe, 1);
    if (ret < 0)
        throw uv_error("Failed to initialize IPC pipe.", ret);

    uv_stdio_container_t child_stdio[3];
    child_stdio[0].flags = (uv_stdio_flags) (UV_CREATE_PIPE | UV_READABLE_PIPE);
    child_stdio[0].data.stream = (uv_stream_t*) input_pipe;
    child_stdio[1].flags = (uv_stdio_flags) (UV_CREATE_PIPE | UV_WRITABLE_PIPE);
    child_stdio[1].data.stream = (uv_stream_t*) output_pipe;
    child_stdio[2].flags = UV_INHERIT_FD;
    child_stdio[2].data.fd = 2;

    static auto exec_completion_cb = [](uv_process_t* process, int64_t exit_status, int term_signal)
    {
        auto state = (ExecutionState*) process->data;

        if (exit_status < 0 || term_signal > 0)
            throw unix_error("Failed to launch process!", exit_status);

        uv_close((uv_handle_t*) state->input_pipe, [](uv_handle_t*){});
        uv_close((uv_handle_t*) state->output_pipe, [](uv_handle_t*){});

        if (state->parser.empty())
            throw runtime_error("No output from execution!");

        auto output = state->parser.front();
        state->parser.pop();
        state->exec_compl_cb_(state, output);
    };

    uv_process_options_t options = {0};
    options.exit_cb = exec_completion_cb;
    options.file = function.c_str();
    options.args = argv;
    options.stdio_count = 3;
    options.stdio = child_stdio;

    if ((ret = uv_spawn(uv_default_loop(), process, &options)) < 0)
        throw uv_error("Failed to spawn process.", ret);

    input_pipe->data = this;
    output_pipe->data = this;


    static auto allocate_buffer_cb = [](uv_handle_t* handle,
                                size_t suggested_size,
                                uv_buf_t* buf)
    {
        auto exec_state = (ExecutionState*) handle->data;
        exec_state->output_buffer_ = string(suggested_size, 0);
        buf->base = &(exec_state->output_buffer_[0]);
        buf->len = suggested_size;
    };

    static auto read_cb = [](uv_stream_t* handle, ssize_t nread,
            const uv_buf_t* buf)
    {
        auto exec_state = (ExecutionState*) handle->data;

        if (nread == UV_EOF)
            return;

        if (nread < 0)
            throw uv_error("Failed to read output from execution!", nread);

        exec_state->parser.parse(exec_state->output_buffer_.substr(0, nread));
        exec_state->output_buffer_.clear();
    };

    static auto write_cb = [](uv_write_t* req, int status)
    {
        auto exec_state = (ExecutionState*) req->data;
        delete req;

        if (status < 0)
            throw uv_error("Failed to send input to execution!", status);

        int ret;
        if ((ret = uv_read_start((uv_stream_t*) exec_state->output_pipe,
                            allocate_buffer_cb, read_cb)) < 0)
            throw uv_error("Failed to read output from execution!", ret);
    };

    s_input_ = string(sizeof(size_t), 0);
    size_t input_len = args.ByteSize();
    *((size_t*)(&s_input_[0])) = input_len;
    s_input_.append(move(args.SerializeAsString()));

    auto input = uv_buf_init(&s_input_[0], s_input_.length());
    uv_write_t* write_req = new uv_write_t();
    write_req->data = this;

    if ((ret = uv_write(write_req, (uv_stream_t*) input_pipe,
                        &input, 1, write_cb)) < 0)
        throw uv_error("Failed to send input to execution!", ret);
}
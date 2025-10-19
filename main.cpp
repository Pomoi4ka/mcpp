#include <iostream>
#include <vector>
#include <string>
#include <tuple>
#include <optional>
#include <string>
#include <unordered_map>

#include <cstdint>
#include <cassert>
#include <cstring>

#include <sys/socket.h>
#include <arpa/inet.h>
#include <poll.h>
#include <time.h>
#include <unistd.h>
#include <setjmp.h>

#include <libco.h>
#include <zlib.h>

const size_t COROUTINE_STACK_CAP = 65536;

struct EventLoop;
struct Coroutine;

static std::vector<unsigned char> compress_bytes(void *bytes, size_t n)
{
    std::vector<unsigned char> result;
    z_stream zstrm{};
    zstrm.avail_in = n;
    zstrm.next_in = (Bytef*) bytes;

    deflateInit(&zstrm, Z_DEFAULT_COMPRESSION);
    for (;;) {
        result.resize(result.size() + 4096);
        zstrm.next_out = result.data() + zstrm.total_out;
        zstrm.avail_out = result.size() - zstrm.total_out;
        int ret = deflate(&zstrm, Z_FINISH);
        switch (ret) {
        case Z_BUF_ERROR:
        case Z_OK: break;
        case Z_STREAM_END: goto exit;
        }
    }
 exit:

    result.resize(zstrm.total_out);
    deflateEnd(&zstrm);

    return result;
}

static std::vector<unsigned char> uncompress_bytes(void *bytes, size_t n)
{
    std::vector<unsigned char> result;
    z_stream zstrm{};
    zstrm.avail_in = n;
    zstrm.next_in = (Bytef*) bytes;

    inflateInit(&zstrm);
    for (;;) {
        result.resize(result.size() + 4096);
        zstrm.next_out = result.data() + zstrm.total_out;
        zstrm.avail_out = result.size() - zstrm.total_out;
        int ret = inflate(&zstrm, Z_FINISH);
        switch (ret) {
        case Z_BUF_ERROR:
        case Z_OK: break;
        case Z_STREAM_END: goto exit;
        }
    }
 exit:

    result.resize(zstrm.total_out);
    inflateEnd(&zstrm);

    return result;
}

template <typename ...Args>
void print(Args&&... args) {
    ((std::cout << args), ...);
    std::cout << std::endl;
}

template <typename ...Args>
void eprint(Args&&... args) {
    ((std::cerr << args), ...);
    std::cerr << std::endl;
}

static uint64_t timespec_to_usecs_from_now(timespec ts) noexcept
{
    timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    now.tv_sec -= ts.tv_sec + 1;
    now.tv_nsec += 1000000000llu - ts.tv_nsec;
    return now.tv_sec * 1000000llu + now.tv_nsec/1000;
}

struct Awaiter {
    enum class Type {
        never,
        always,
        once,
        poll,
        interval,
    };

    enum class poll_result {
        polling,
        ready,
        done,
    };

private:
    explicit Awaiter(Type type) noexcept
        : type(type)
    {}

public:
    explicit Awaiter(pollfd pfd, int timeout) noexcept
        : type(Type::poll)
        , t_pollfd{pfd, timeout, 0}
    {}

    explicit Awaiter(pollfd pfd) noexcept
        : Awaiter(pfd, -1)
    {}

    explicit Awaiter() noexcept
        : Awaiter(Type::always)
    {}

    static Awaiter interval(uint64_t usecs) noexcept {
        Awaiter a(Type::interval);
        a.t_interval.usecs = usecs;
        return a;
    }

    static Awaiter never() noexcept {
        return Awaiter(Type::never);
    }

    static Awaiter always() noexcept {
        return Awaiter(Type::always);
    }

    static Awaiter once() noexcept {
        return Awaiter(Type::once);
    }

    void await(const Coroutine *ctx) noexcept;

    void set_timeout(int msecs) noexcept {
        assert(type == Type::poll);
        t_pollfd.timeout = msecs;
    }

    void set_events(short events) noexcept {
        assert(type == Type::poll);
        t_pollfd.pfd.events = events;
    }

    void set_pollfd(pollfd pfd) noexcept {
        type = Type::poll;
        t_pollfd.pfd = pfd;
    }

    void set_pollfd(int fd, short events, int timeout) noexcept {
        type = Type::poll;
        t_pollfd.pfd.fd = fd;
        t_pollfd.pfd.events = events;
        t_pollfd.pfd.revents = 0;
        t_pollfd.timeout = timeout;
        t_pollfd.revents = 0;
    }

    pollfd get_pollfd() const {
        assert(type == Type::poll);
        return t_pollfd.pfd;
    }

    uint64_t elapsed_usecs() const noexcept {
        assert(begin_await_at);
        return timespec_to_usecs_from_now(*begin_await_at);
    }

    std::optional<uint64_t> remaining_usecs() const noexcept {
        uint64_t elapsed;
        switch (type) {
        case Type::interval:
            elapsed = elapsed_usecs();
            if (t_interval.usecs < elapsed) return 0;
            return {t_interval.usecs - elapsed};
        case Type::once:
        case Type::always:
            return 0;
        case Type::never:
            return {};
        case Type::poll:
            if (t_pollfd.timeout == -1) return {};
            elapsed = elapsed_usecs();
            if ((uint64_t)t_pollfd.timeout * 1000 < elapsed) return {0};
            return {t_pollfd.timeout * 1000 - elapsed};
        }
        abort();
    }

    short revents() const noexcept {
        assert(type == Type::poll);
        return t_pollfd.revents;
    }

    poll_result poll() noexcept {
        switch (type) {
        case Type::never: return poll_result::done;
        case Type::always: return poll_result::ready;
        case Type::once:
            type = Type::never;
            return poll_result::ready;
        case Type::interval:
            if (!begin_await_at) return poll_result::ready;
            if (!*remaining_usecs()) {
                begin_await_at = {};
                return poll_result::ready;
            }
            break;
        case Type::poll:
            if (!begin_await_at) return poll_result::ready;
            t_pollfd.revents = t_pollfd.pfd.revents;
            if (auto rem = remaining_usecs()) {
                if (!*rem) return poll_result::ready;
            }
            if (t_pollfd.pfd.revents) {
                begin_await_at = {};
                t_pollfd.pfd.revents = 0;
                return poll_result::ready;
            }
            break;
        }

        return poll_result::polling;
    }

    Type get_type() const {
        return type;
    }
protected:
    Type type;
    std::optional<timespec> begin_await_at;
    union {
        struct {
            pollfd pfd;
            int timeout;
            short revents;
        } t_pollfd;
        struct {
            uint64_t usecs;
        } t_interval;
    };
};

template <typename, typename, int = 0, typename...>
struct TupleCaller;

template <typename F, int n, typename Head, typename ...Ts, typename ...Args>
struct TupleCaller<F, std::tuple<Head, Ts...>, n, Args...>
{
    template <typename T>
    constexpr void operator()(F f, T t, Args&&... args) noexcept {
        TupleCaller<F, std::tuple<Ts...>, n+1, Args..., Head>{
        }(f, std::move(t), std::forward<Args>(args)..., std::forward<Head>(std::get<n>(t)));
    }
};

template <typename F, int n, typename ...Args>
struct TupleCaller<F, std::tuple<>, n, Args...>
{
    template <typename T>
    constexpr void operator()(F f, T, Args&&... args) noexcept {
        f(std::forward<Args>(args)...);
    }
};

struct Coroutine {
    char stack[COROUTINE_STACK_CAP];
    EventLoop *event_loop;
    cothread_t exec_ctx;

    Awaiter awaiter;
    bool finished = false;

    [[noreturn]] void exit() noexcept;
    void await() noexcept {
        awaiter.await(this);
    }

    virtual ~Coroutine() = default;
    explicit Coroutine() = default;

    Coroutine(Coroutine const &) = delete;

    template <typename ...Args, typename FArgs>
    static Coroutine *create(EventLoop &event_loop, Awaiter awaiter,
                             void (*f)(Coroutine *, Args...) noexcept, FArgs args)
        noexcept
    {
        struct WrapperClosure {
            void (*func)(Coroutine *, Args...);
            FArgs args;
            cothread_t parent_ctx;
            Coroutine *ctx;
        };

        auto ctx = new Coroutine;

        WrapperClosure wrapper_closure = {
            f,
            std::forward<FArgs>(args),
            co_active(),
            ctx
        };
        thread_local static WrapperClosure *volatile wrapper_closure_ptr = &wrapper_closure;

        ctx->event_loop = &event_loop;
        ctx->exec_ctx = co_derive(ctx->stack, sizeof ctx->stack, []() noexcept {
            auto func = wrapper_closure_ptr->func;
            auto args = std::forward<FArgs>(wrapper_closure_ptr->args);
            auto ctx = wrapper_closure_ptr->ctx;

            co_switch(wrapper_closure_ptr->parent_ctx);

            TupleCaller<decltype(func), FArgs, 0, decltype(ctx)>{
            }(func, std::forward<FArgs>(args), std::move(ctx));
            ctx->exit();
        });
        ctx->awaiter = awaiter;

        co_switch(ctx->exec_ctx);

        return ctx;
    }

    template <typename T, typename ...Args, typename FArgs>
    static Coroutine *create(EventLoop &event_loop, Awaiter awaiter,
                             void (T::*f)(Args...) noexcept, FArgs args)
        noexcept
    {
        struct WrapperClosure {
            void (T::*func)(Args...) noexcept;
            FArgs args;
            cothread_t parent_ctx;
            T *ctx;
        };

        auto ctx = new T();

        WrapperClosure wrapper_closure = {
            f,
            std::forward<FArgs>(args),
            co_active(),
            ctx
        };
        thread_local static WrapperClosure *volatile wrapper_closure_ptr = &wrapper_closure;

        ctx->event_loop = &event_loop;
        ctx->exec_ctx = co_derive(ctx->stack, sizeof ctx->stack, []() noexcept {
            auto func = wrapper_closure_ptr->func;
            auto args = std::forward<FArgs>(wrapper_closure_ptr->args);
            auto ctx = wrapper_closure_ptr->ctx;

            co_switch(wrapper_closure_ptr->parent_ctx);

            struct {
                T* ctx;
                void (T::*func)(Args...) noexcept;

                void operator()(Args... args) noexcept {
                    (ctx->*func)(std::forward<Args>(args)...);
                }
            } fn = {ctx, func};

            TupleCaller<decltype(fn), FArgs>{}(fn, std::forward<FArgs>(args));
            ctx->exit();
        });
        ctx->awaiter = awaiter;

        co_switch(ctx->exec_ctx);

        return ctx;
    }
};

struct EventLoop {
    std::vector<Coroutine *> ctx_list;
    std::vector<size_t> finished;
    cothread_t exec_ctx;
    bool exit = false;

    explicit EventLoop() noexcept
        : ctx_list()
        , exec_ctx(co_active())
    {}

    template <typename F, typename ...Args>
    void add_ctx(F f, Args&&... args)
        noexcept
    {
        add_ctx(Awaiter::once(), f, std::forward<Args>(args)...);
    }

    template <typename F, typename ...Args>
    void add_ctx(Awaiter awaiter, F f, Args&&... args)
        noexcept
    {
        auto ctx = Coroutine::create(*this, awaiter, f, std::tuple(std::forward<Args>(args)...));
        ctx_list.push_back(ctx);
    }

    void run() noexcept {
        while (!exit && ctx_list.size()) {
            for (auto i = finished.cend(); i != finished.cbegin(); ) {
                std::swap(ctx_list[ctx_list.size()-1], ctx_list[*--i]);
                delete ctx_list[ctx_list.size()-1];
                ctx_list.pop_back();
            }
            finished.clear();

            uint64_t usecs_min = ~0;
            std::vector<pollfd> pfds;
            std::vector<int> to_switch;

            for (size_t i = 0; i < ctx_list.size(); ++i) {
                auto ctx = ctx_list[i];
                switch (ctx->awaiter.get_type()) {
                case Awaiter::Type::interval:
                    switch (ctx->awaiter.poll()) {
                    case Awaiter::poll_result::done: break;
                    case Awaiter::poll_result::polling:
                        usecs_min = std::min(usecs_min, *ctx->awaiter.remaining_usecs());
                        break;
                    case Awaiter::poll_result::ready:
                        to_switch.push_back(i);
                        break;
                    }
                    break;

                case Awaiter::Type::always:
                case Awaiter::Type::once:
                    to_switch.push_back(i);
                    break;

                case Awaiter::Type::never:
                    break;

                case Awaiter::Type::poll:
                    if (ctx->awaiter.poll() == Awaiter::poll_result::ready) {
                        to_switch.push_back(i);
                        break;
                    }
                    if (auto remaining = ctx->awaiter.remaining_usecs()) {
                        usecs_min = std::min(usecs_min, *remaining);
                    }
                    pfds.push_back(ctx->awaiter.get_pollfd());
                    break;
                }
            }

            for (auto i : to_switch) {
                switch_ctx(i);
            }
            if (to_switch.size()) continue;

            size_t n = ctx_list.size();
            if (pfds.size()) {
                poll(pfds.data(), pfds.size(), usecs_min == ~0llu ? -1 : usecs_min / 1000);
                for (size_t i = 0, j = 0; i < n; ++i) {
                    if (ctx_list[i]->awaiter.get_type() != Awaiter::Type::poll) continue;
                    auto &awaiter = ctx_list[i]->awaiter;
                    auto pfd = awaiter.get_pollfd();
                    assert(pfd.fd == pfds[j].fd);
                    awaiter.set_pollfd(pfds[j]);
                    ++j;
                }
            } else if (usecs_min != ~0llu) {
                usleep(usecs_min);
            }

            for (size_t i = 0; i < n; ++i) {
                switch (ctx_list[i]->awaiter.poll()) {
                case Awaiter::poll_result::polling:
                case Awaiter::poll_result::done: break;
                case Awaiter::poll_result::ready:
                    switch_ctx(i);
                    break;
                }
            }
        }
    }

    ~EventLoop() {
        for (auto ctx : ctx_list) {
            delete ctx;
        }
    }

private:
    void switch_ctx(size_t i) noexcept {
        auto ctx = ctx_list[i];
        if (ctx->finished) goto finished;
        co_switch(ctx_list[i]->exec_ctx);
        if (ctx->finished)
        finished:
            finished.push_back(i);
    }
};

[[noreturn]] void Coroutine::exit() noexcept {
    finished = true;
    co_switch(event_loop->exec_ctx);
    abort();
}

void Awaiter::await(const Coroutine *ctx) noexcept {
    begin_await_at = {timespec{}};
    clock_gettime(CLOCK_MONOTONIC, &*begin_await_at);
    while (poll() != poll_result::ready) {
        co_switch(ctx->event_loop->exec_ctx);
    }
}

struct Socket {
    int fd;

    ~Socket() noexcept {
        close(fd);
    }

    int read(void *dst, int n) const noexcept {
        return ::read(fd, dst, n);
    }

    int write(const void *src, int n) const noexcept {
        return ::write(fd, src, n);
    }
};

struct BufIo {
    struct Handler {
        virtual int read(void *dst, int n) = 0;
        virtual int write(const void *src, int n) = 0;
    };

    Handler *handler;
    unsigned int readavail = 0, writeavail = 0;
    unsigned int readpos = 0, writepos = 0;
    std::vector<unsigned char> readbuf, writebuf;

    explicit BufIo(Handler *handler, size_t bufsize = 4096) noexcept
        : handler(handler)
        , readbuf(bufsize), writebuf(bufsize)
    {}

    void fetch() {
        size_t avail = readbuf.size() - readpos;
        int ret = handler->read(&readbuf[readpos], avail);
        readavail += ret;
    }

    void flush(int flush_all = 1) {
        while (flush_all && writeavail) {
            size_t pos = writepos - writeavail;
            int ret = handler->write(&writebuf[pos], writeavail);
            writeavail -= ret;
        }
        writepos %= writebuf.size();
    }

    void write(const void *src, int n) {
        unsigned int off = 0;
        auto buf = reinterpret_cast<const unsigned char*>(src);
        while ((int)off < n) {
            if (writeavail == writebuf.size()) flush(0);
            unsigned int avail = writebuf.size() - writepos;
            unsigned int to_copy = std::min(n - off, avail);
            memcpy(&writebuf[writepos], buf + off, to_copy);
            off += to_copy;
            writeavail += to_copy;
            writepos += to_copy;
        }
    }

    void read(void *dst, int n) {
        unsigned int off = 0;
        auto buf = reinterpret_cast<unsigned char*>(dst);
        while ((int)off < n) {
            if (!readavail) fetch();
            unsigned int to_copy = std::min(n - off, readavail);
            memcpy(buf + off, &readbuf[readpos], to_copy);
            readpos = (readpos + to_copy) % readbuf.size();
            off += to_copy;
            readavail -= to_copy;
        }
    }
};

struct AsyncSocket : Socket, BufIo::Handler {
    enum class error {
        timedout,
        closed,
        error,
    };

    Coroutine &c;
    int timeout;

    explicit AsyncSocket(Coroutine *c, int timeout) noexcept
        : c(*c)
        , timeout(timeout)
    {}

    void set_timeout(int timeout) noexcept {
        this->timeout = timeout;
    }

    int read(void *dst, int n) override {
        c.awaiter.set_pollfd(this->fd, POLLIN, timeout);
        c.await();
        if (!(c.awaiter.revents() & POLLIN)) throw error::timedout;
        int ret = this->Socket::read(dst, n);
        if (ret == 0) throw error::closed;
        if (ret < 0) throw error::error;
        return ret;
    }

    int write(const void *src, int n) override {
        c.awaiter.set_pollfd(this->fd, POLLOUT, timeout);
        c.await();
        if (!(c.awaiter.revents() & POLLOUT)) throw error::timedout;
        int ret = this->Socket::write(src, n);
        if (ret < 0) throw error::error;
        return ret;
    }
};

namespace Varint {
    const int SEGMENT_BITS = 0x7F;
    const int CONTINUE_BIT = 0x80;

    enum class error {
        VarintTooBig,
    };

    template <typename Writer>
    void write(Writer&& w, unsigned int value) {
        while (true) {
            if ((value & ~SEGMENT_BITS) == 0) {
                w.write(&value, 1);
                return;
            }

            unsigned char a = (value & SEGMENT_BITS) | CONTINUE_BIT;
            w.write(&a, 1);
            value >>= 7;
        }
    }

    template <typename Reader>
    int read(Reader&& r) {
        int value = 0;
        int position = 0;
        unsigned char currentByte;

        while (true) {
            r.read(&currentByte, 1);
            value |= (currentByte & SEGMENT_BITS) << position;

            if ((currentByte & CONTINUE_BIT) == 0) break;
            position += 7;
            if (position >= 32) throw error::VarintTooBig;
        }

        return value;
    }
}

template <typename T>
void endianswap(T *value)
{
    char *b = reinterpret_cast<char *>(value);
    for (size_t i = 0; i < sizeof(T)/2; ++i) {
        char t = b[i];
        char& a = b[sizeof(T) - i - 1];
        b[i] = a;
        a = t;
    }
}

struct ByteBuffer : std::vector<unsigned char> {
    size_t pos = 0;

    unsigned char *front() {
        return data() + pos;
    }

    const unsigned char *front() const {
        return data() + pos;
    }

    void read(void *dst, size_t n) noexcept {
        if (n + pos > size()) n = size() - pos;
        memcpy(dst, front(), n);
        pos += n;
    }

    void write(const void *src, size_t n) {
        if (n + pos > size()) resize(pos + n);
        memcpy(front(), src, n);
        pos += n;
    }

    template <typename T>
    T get() {
        T value;
        read(&value, sizeof(T));
        endianswap(&value);
        return value;
    }

    template <typename T>
    void put(T value) {
        endianswap(&value);
        write(&value, sizeof(T));
    }

    template <typename T>
    T get_be() {
        T value;
        read(&value, sizeof(T));
        return value;
    }
};

std::ostream& operator<<(std::ostream& s, std::vector<unsigned char> &v)
{
    s << "{";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i > 0) s << ", ";
        s << std::hex << (int)v[i];
    }
    s << "}";
    return s;
}

struct UUID {
    uint64_t low, high;
};

namespace NBT {
    enum class TagType {
        End,
        Byte,
        Short,
        Int,
        Long,
        Float,
        Double,
        Byte_Array,
        String,
        List,
        Compound,
        Int_Array,
        Long_Array,
    };

    class Tag {
    public:
        virtual ~Tag() = default;
        TagType get_type() const {
            return type;
        }

    protected:
        Tag(TagType type)
            : type(type)
        {}

        TagType type;
    };

    struct ByteArrayTag : Tag {
        ByteArrayTag(std::vector<char> value)
            : Tag(TagType::Byte_Array)
            , value(value) {}
        std::vector<char> value;
    };

    struct IntArrayTag : Tag {
        IntArrayTag(std::vector<int> value)
            : Tag(TagType::Int_Array)
            , value(value) {}
        std::vector<int> value;
    };

    struct LongArrayTag : Tag {
        LongArrayTag(std::vector<int64_t> value)
            : Tag(TagType::Long_Array)
            , value(value) {}
        std::vector<int64_t> value;
    };

    struct StringTag : Tag {
        StringTag(std::string value)
            : Tag(TagType::String)
            , value(value) {}
        std::string value;
    };

    struct ListTag : Tag {
        ListTag(TagType list_type, std::vector<Tag *> value)
            : Tag(TagType::List)
            , list_type(list_type)
            , value(value)
        {}

        ~ListTag() {
            for (auto p : value) {
                delete p;
            }
        }

        TagType list_type;
        std::vector<Tag *> value;
    };

    struct CompoundTag : Tag {
        CompoundTag(std::unordered_map<std::string, Tag *> value)
            : Tag(TagType::Compound)
            , value(value) {}

        ~CompoundTag() {
            for (auto &[_, p] : value) {
                delete p;
            }
        }

        std::unordered_map<std::string, Tag *> value;
    };

    struct ByteTag : Tag {
        ByteTag(char value) : Tag(TagType::Byte), value(value) {}
        char value;
    };
    struct ShortTag : Tag {
        ShortTag(short value) : Tag(TagType::Byte), value(value) {}
        short value;
    };
    struct IntTag : Tag {
        IntTag(int value) : Tag(TagType::Byte), value(value) {}
        int value;
    };
    struct LongTag : Tag {
        LongTag(long value) : Tag(TagType::Byte), value(value) {}
        int64_t value;
    };
    struct FloatTag : Tag {
        FloatTag(float value) : Tag(TagType::Byte), value(value) {}
        float value;
    };
    struct DoubleTag : Tag {
        DoubleTag(double value) : Tag(TagType::Byte), value(value) {}
        double value;
    };

    static Tag *tag_from_bytes(ByteBuffer &bb, TagType type) {
        switch (type) {
        default: break;
        case TagType::Byte: return new ByteTag(bb.get<char>());
        case TagType::Short: return new ShortTag(bb.get<short>());
        case TagType::Int: return new IntTag(bb.get<int>());
        case TagType::Long: return new LongTag(bb.get<int64_t>());
        case TagType::Float: return new FloatTag(bb.get<float>());
        case TagType::Double: return new DoubleTag(bb.get<double>());
        case TagType::Byte_Array: {
            int len = bb.get<int>();
            std::vector<char> bytes(len);
            bb.read(bytes.data(), bytes.size());
            return new ByteArrayTag(bytes);
        } break;
        case TagType::Int_Array: {
            int len = bb.get<int>();
            std::vector<int> value(len);
            bb.read(value.data(), value.size());
            return new IntArrayTag(value);
        } break;
        case TagType::Long_Array: {
            int len = bb.get<int64_t>();
            std::vector<int64_t> value(len);
            bb.read(value.data(), value.size());
            return new LongArrayTag(value);
        } break;
        case TagType::String: {
            auto len = bb.get<unsigned short>();
            std::string value;
            value.resize(len);
            bb.read(value.data(), value.size());
            return new StringTag(value);
        } break;
        case TagType::List: {
            TagType type = static_cast<TagType>(bb.get<char>());
            auto len = bb.get<int>();
            std::vector<Tag *> value(len);
            for (int i = 0; i < len; ++i) {
                value[i] = tag_from_bytes(bb, type);
            }
            return new ListTag(type, value);
        } break;
        case TagType::Compound: {
            TagType type;
            std::unordered_map<std::string, Tag *> value;
            for (;;) {
                type = static_cast<TagType>(bb.get<char>());
                if (type == TagType::End) break;
                auto len = bb.get<unsigned short>();
                std::string name;
                name.resize(len);
                bb.read(name.data(), name.size());

                value.insert({name, tag_from_bytes(bb, type)});
            }
            return new CompoundTag(value);
        } break;
        }
        return nullptr;
    }

    static Tag *tag_from_bytes(ByteBuffer &bb) {
        TagType type = static_cast<TagType>(bb.get<char>());
        return tag_from_bytes(bb, type);
    }

    static void serialize_tag(const Tag *tag, ByteBuffer &bb, bool put_type = true) {
        auto type = tag->get_type();

        if (put_type) bb.put(static_cast<char>(type));

        switch (type) {
        case TagType::String: {
            const StringTag *str = reinterpret_cast<const StringTag *>(tag);
            auto len = str->value.size();
            bb.put<unsigned short>(len);
            bb.write(str->value.data(), len);
        } break;
        case TagType::Compound: {
            const CompoundTag *c = reinterpret_cast<const CompoundTag *>(tag);
            for (auto &[name, tag] : c->value) {
                bb.put(static_cast<char>(tag->get_type()));
                auto len = name.size();
                bb.put<unsigned short>(len);
                bb.write(name.data(), len);
                serialize_tag(tag, bb, false);
            }
            bb.put(static_cast<char>(TagType::End));
        } break;
        case TagType::List: {
            const ListTag *c = reinterpret_cast<const ListTag *>(tag);
            bb.put(static_cast<char>(c->list_type));
            bb.put<int>(c->value.size());
            for (auto tag : c->value) {
                serialize_tag(tag, bb, false);
            }
        } break;
        default: assert(0 && "not implemented");
        }
    }
}

struct PacketStream : BufIo {
    enum class error {
        InvalidPacket,
    };

    ByteBuffer construct_packet_buffer;
    ByteBuffer receive_packet_buffer;
    int compression_threshold = -1;

    explicit PacketStream(BufIo::Handler *handler)
        : BufIo(handler)
        , construct_packet_buffer()
        , receive_packet_buffer()
    {}

    template <typename T>
    T get() {
        T value;
        receive_packet_buffer.read(&value, sizeof(T));
        endianswap(&value);
        return value;
    }

    void packet_begin(int packet_id) {
        construct_packet_buffer.clear();
        construct_packet_buffer.pos = 0;
        Varint::write(construct_packet_buffer, packet_id);
    }

    void packet_end() {
        const auto &buf = construct_packet_buffer;
        if (compression_threshold < 0) {
            Varint::write(*this, buf.size());
            write(buf.data(), buf.size());
            flush();
        } else {
            assert(0 && "not implemented");
        }
    }

    void push_string(std::string_view string) {
        auto &buf = construct_packet_buffer;
        push_varint(string.size());
        buf.write(string.data(), string.size());
    }

    void push_tag(const NBT::Tag *tag) {
        NBT::serialize_tag(tag, construct_packet_buffer);
    }

    void push_varint(int value) {
        Varint::write(construct_packet_buffer, value);
    }

    template <typename T>
    void push(T value) {
        endianswap(&value);
        construct_packet_buffer.write(&value, sizeof(T));
    }

    void push_uuid(UUID uuid) {
        push(uuid.high);
        push(uuid.low);
    }

    int get_varint() {
        return Varint::read(receive_packet_buffer);
    }

    UUID get_uuid() {
        UUID result;
        receive_packet_buffer.read(&result, sizeof result);
        endianswap(&result);
        return result;
    }

    std::string_view get_string() {
        size_t len = get_varint();
        const char *data = reinterpret_cast<const char *>(receive_packet_buffer.front());
        receive_packet_buffer.pos += len;
        return {data, len};
    }

    int receive_packet() {
        int length = Varint::read(*this);
        if (length < 0) throw error::InvalidPacket;
        receive_packet_buffer.pos = 0;
        receive_packet_buffer.resize(length);
        read(receive_packet_buffer.data(), length);
        if (compression_threshold < 0) {
            return get_varint();
        } else {
            assert(0 && "not implemented");
        }
    }
};

struct Connection
    : Coroutine
    , PacketStream
{
    enum class State {
        Handshake,
        Login,
        Configuration,
    };

    explicit Connection() noexcept
        : Coroutine()
        , PacketStream(&sock)
        , sock(this, 30000)
    {}

    void exit() {
        shutdown(sock.fd, SHUT_RD);
        awaiter.set_events(POLLIN|POLLHUP);
        await();
        Coroutine::exit();
    }

    void not_implemented(std::string_view func = "") {

        switch (state) {
        case State::Handshake:
        case State::Login: {
            std::string ans = "{\"text\": \"Not implemented\"}";
            if (func.size()) {
                ans = "{\"text\": \"Not implemented: ";
                ans += func;
                ans += "\"}";
            }
            packet_begin(0x00);
            push_string(ans);
        } break;
        case State::Configuration: {
            packet_begin(0x02);
            NBT::Tag *tag;
            if (func.size()) {
                using namespace NBT;
                tag = new CompoundTag({
                        {"text", new StringTag("Not implemented: ")},
                        {"extra",
                         new ListTag(TagType::Compound, {
                                 new CompoundTag({
                                         {"text", new StringTag(std::string{func})},
                                         {"color", new StringTag("red")}
                                     })
                             })}
                    });
            } else {
                tag = new NBT::StringTag("Not implemented");
            }
            push_tag(tag);
            delete tag;
        } break;
        }
        packet_end();

        exit();
    }

    void state_login() {
        int id = receive_packet();
        switch (id) {
        case 0x00: {
            auto name = get_string(); // Player name
            auto uuid = get_uuid();
            print("Name: ", name);

            packet_begin(0x02);
            push_uuid(uuid);
            push_string(name);
            push_varint(0);
            packet_end();
        } break;
        case 0x03:
            state = State::Configuration;
            break;
        }
    }

    void state_handshake() {
        int id = receive_packet();
        switch (id) {
        case 0x00: {
            get_varint(); // ProtoVer
            get_string(); // Server addr
            get<short>(); // Port
            int intent = get_varint();
            switch (intent) {
            case 0x02: state = State::Login; break;
            default: not_implemented(); break;
            }
        } break;
        }
    }

    void state_router() {
        while (true) {
            switch (state) {
            case State::Handshake: state_handshake(); break;
            case State::Login: state_login(); break;
            case State::Configuration: not_implemented("Configuration"); break;
            }
        }
    }

    void create(int sock, sockaddr_in addr) noexcept {
        this->sock.fd = sock;
        print("Accepted ", inet_ntoa(addr.sin_addr), ":", ntohs(addr.sin_port));

        state_router();
    }
protected:
    AsyncSocket sock;
    State state = State::Handshake;
};

struct Listener : Coroutine {
    int fd;

    ~Listener() noexcept {
        close(fd);
    }

    void listen(int port) noexcept {
        fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (fd < 0) {
            eprint("Failed to create socket: ", strerror(errno));
            return;
        }

        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);

        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = 0;

        if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0) {
            eprint("Failed to bind: ", strerror(errno));
            return;
        }

        if (::listen(fd, 5) < 0) {
            eprint("Failed to listen: ", strerror(errno));
            return;
        }

        print("Listening to port ", port);

        awaiter.set_pollfd(fd, POLLIN, -1);
        for (;;) {
            await();
            socklen_t n = sizeof addr;
            int sock = accept(fd, reinterpret_cast<sockaddr*>(&addr), &n);
            if (sock < 0) {
                eprint("Failed to accept connection: ", strerror(errno));
                return;
            }

            event_loop->add_ctx(&Connection::create, sock, addr);
        }
    }
};

int main()
{
    auto event_loop = EventLoop();
    event_loop.add_ctx(&Listener::listen, 6969);
    event_loop.run();

    return 0;
}

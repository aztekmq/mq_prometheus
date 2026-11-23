#include <cmqc.h>
#include <cmqxc.h>
#include <arpa/inet.h>
#include <poll.h>
#include <signal.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using Fields = std::map<std::string, std::string>;
using Tag = std::array<unsigned char, MQ_CORREL_ID_LENGTH>;
using Clock = std::chrono::steady_clock;
constexpr int maxMessageBytes = 4 * 1024 * 1024;
constexpr int maxRunSeconds = 3600;

struct MQError : std::runtime_error {
    MQLONG reason;
    MQError(const std::string& operation, MQLONG code)
        : std::runtime_error(operation + " failed (MQRC " + std::to_string(code) + ")"), reason(code) {}
};

void check(const std::string& operation, MQLONG completion, MQLONG reason) {
    if (completion != MQCC_OK) throw MQError(operation, reason);
}

template <size_t N>
void mqField(char (&field)[N], const std::string& value) {
    if (value.size() > N) throw std::runtime_error("MQ field exceeds its maximum length");
    std::fill_n(field, N, ' ');
    std::copy(value.begin(), value.end(), field);
}

struct Target {
    std::string manager, address, channel, user, password;
};

class Connection {
    MQHCONN connection = MQHC_UNUSABLE_HCONN;
    std::map<std::string, MQHOBJ> queues;

public:
    explicit Connection(const Target& target) {
        MQCD cd = {MQCD_CLIENT_CONN_DEFAULT};
        mqField(cd.ConnectionName, target.address);
        mqField(cd.ChannelName, target.channel);
        MQCSP security = {MQCSP_DEFAULT};
        MQCNO options = {MQCNO_DEFAULT};
        options.Version = MQCNO_VERSION_5;
        options.ClientConnPtr = &cd;
        if (!target.user.empty()) {
            security.AuthenticationType = MQCSP_AUTH_USER_ID_AND_PWD;
            security.CSPUserIdPtr = const_cast<char*>(target.user.data());
            security.CSPUserIdLength = target.user.size();
            security.CSPPasswordPtr = const_cast<char*>(target.password.data());
            security.CSPPasswordLength = target.password.size();
            options.SecurityParmsPtr = &security;
        }
        MQCHAR48 manager;
        mqField(manager, target.manager);
        MQLONG completion, reason;
        MQCONNX(manager, &options, &connection, &completion, &reason);
        if (completion != MQCC_OK && connection != MQHC_UNUSABLE_HCONN) {
            MQLONG cleanupCompletion, cleanupReason;
            MQDISC(&connection, &cleanupCompletion, &cleanupReason);
        }
        check("MQCONNX " + target.manager, completion, reason);
    }

    ~Connection() {
        MQLONG completion, reason;
        MQDISC(&connection, &completion, &reason);
    }

    MQHOBJ open(const std::string& name, bool put, bool get) {
        if (auto found = queues.find(name); found != queues.end()) return found->second;

        MQOD object = {MQOD_DEFAULT};
        mqField(object.ObjectName, name);
        MQHOBJ handle = MQHO_UNUSABLE_HOBJ;
        MQLONG completion, reason;
        MQLONG options = MQOO_FAIL_IF_QUIESCING;
        if (put) options |= MQOO_OUTPUT;
        if (get) options |= MQOO_INPUT_SHARED;
        MQOPEN(connection, &object, options, &handle, &completion, &reason);
        check("MQOPEN " + name, completion, reason);
        queues.emplace(name, handle);
        return handle;
    }

    Tag put(MQHOBJ handle, const Tag& tag, std::string& payload, MQLONG persistence) {
        MQMD descriptor = {MQMD_DEFAULT};
        std::memcpy(descriptor.CorrelId, tag.data(), tag.size());
        std::memcpy(descriptor.Format, MQFMT_STRING, MQ_FORMAT_LENGTH);
        descriptor.CodedCharSetId = 1208;
        descriptor.Persistence = persistence;
        MQPMO options = {MQPMO_DEFAULT};
        options.Options = MQPMO_NEW_MSG_ID | MQPMO_NO_SYNCPOINT | MQPMO_FAIL_IF_QUIESCING;
        MQLONG completion, reason;
        MQPUT(connection, handle, &descriptor, &options, payload.size(), payload.data(), &completion, &reason);
        check("MQPUT", completion, reason);
        Tag messageId;
        std::memcpy(messageId.data(), descriptor.MsgId, messageId.size());
        return messageId;
    }

    std::optional<size_t> get(MQHOBJ handle, const Tag& tag, std::vector<char>& buffer,
                              const std::optional<Tag>& messageId) {
        MQMD descriptor = {MQMD_DEFAULT};
        std::memcpy(descriptor.CorrelId, tag.data(), tag.size());
        MQGMO options = {MQGMO_DEFAULT};
        options.Version = MQGMO_VERSION_2;
        options.MatchOptions = MQMO_MATCH_CORREL_ID;
        if (messageId) {
            std::memcpy(descriptor.MsgId, messageId->data(), messageId->size());
            options.MatchOptions |= MQMO_MATCH_MSG_ID;
        }
        options.Options = MQGMO_NO_WAIT | MQGMO_NO_SYNCPOINT | MQGMO_FAIL_IF_QUIESCING;
        MQLONG actual, completion, reason;
        MQGET(connection, handle, &descriptor, &options, buffer.size(), buffer.data(), &actual, &completion, &reason);
        if (reason == MQRC_NO_MSG_AVAILABLE) return std::nullopt;
        check("MQGET", completion, reason);
        return actual;
    }
};

std::string json(const std::string& value) {
    std::ostringstream out;
    out << '"';
    for (unsigned char c : value) {
        if (c == '"' || c == '\\') out << '\\' << c;
        else if (c < 32) out << "\\u00" << std::hex << std::setw(2) << std::setfill('0') << int(c);
        else out << c;
    }
    out << '"';
    return out.str();
}

std::string hex(const Tag& tag) {
    std::ostringstream out;
    for (auto byte : tag) out << std::hex << std::setw(2) << std::setfill('0') << int(byte);
    return out.str();
}

int number(const std::string& text, int min, int max) {
    int value = 0;
    auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size() || value < min || value > max) {
        throw std::runtime_error("Numeric setting outside its allowed range");
    }
    return value;
}

Tag workloadTag(const std::string& supplied, bool getOnly) {
    Tag tag{};
    if (!getOnly) {
        if (getrandom(tag.data(), tag.size(), 0) != static_cast<ssize_t>(tag.size())) {
            throw std::runtime_error("Could not generate a run ID");
        }
        // Reserve a prefix so get-only mode accepts IDs generated by this application.
        std::memcpy(tag.data(), "MQTRAF01", 8);
        return tag;
    }
    if (supplied.size() != tag.size() * 2) throw std::runtime_error("Enter a valid traffic run ID");
    for (size_t i = 0; i < tag.size(); ++i) {
        const char* start = supplied.data() + i * 2;
        auto [end, error] = std::from_chars(start, start + 2, tag[i], 16);
        if (error != std::errc{} || end != start + 2) throw std::runtime_error("Invalid run ID");
    }
    if (std::memcmp(tag.data(), "MQTRAF01", 8) != 0) throw std::runtime_error("Invalid traffic run ID");
    return tag;
}

std::string queueName(int queue) {
    std::ostringstream name;
    name << "APP.TEST.LOCAL." << std::setw(3) << std::setfill('0') << queue;
    return name.str();
}

struct Counts {
    uint64_t puts = 0, gets = 0, empty = 0, errors = 0, done = 0;
    uint64_t bytesPut = 0, bytesGot = 0;
    uint64_t persistentPuts = 0, persistentGets = 0;
    uint64_t slowestVisitMs = 0;
    std::string error;
};

struct Load {
    int rate, batch, connections;
    std::string payload;
    MQLONG persistence = MQPER_PERSISTENCE_AS_Q_DEF;
};

struct Profile {
    int rate, batch, bytes, connections;
};

const std::map<std::string, Profile> profiles = {
    {"light", {10, 1, 256, 1}},
    {"medium", {50, 2, 4096, 1}},
    {"heavy", {400, 8, 65536, 4}},
    {"extreme", {0, 32, 256 * 1024, 32}},
};
constexpr int extremePersistentBytes = 1024 * 1024;
constexpr int adaptiveMinWorkers = 8;
constexpr int adaptiveMaxWorkers = 64;
constexpr int adaptiveMinBytes = 128 * 1024;
constexpr int adaptiveMaxBytes = 1024 * 1024;
constexpr auto adaptiveWindow = 8s;

struct CpuSample {
    uint64_t total = 0, idle = 0;
};

CpuSample hostCpu() {
    std::ifstream stats("/proc/stat");
    std::string label;
    std::array<uint64_t, 8> ticks{};
    if (!(stats >> label) || label != "cpu") return {};
    CpuSample sample;
    for (auto& tick : ticks) {
        if (!(stats >> tick)) return {};
        sample.total += tick;
    }
    sample.idle = ticks[3] + ticks[4];
    return sample;
}

struct Run {
    std::mutex guard;
    std::atomic<bool> stop{false};
    std::array<std::array<Counts, 500>, 2> queues;
    std::array<Counts, 2> managers;
    std::array<std::string, 2> states{"Idle", "Idle"};
    std::array<int, 2> remaining{}, connected{}, configured{}, failures{};
    std::array<std::atomic<unsigned>, 2> nextQueue{};
    std::atomic<int> adaptiveWorkers{0}, adaptiveBytes{0};
    std::vector<std::thread> workers;
    std::thread tuner;
    Tag tag{};
    std::string mode, profileName;
    bool persistent = false;
    double cpuPercent = -1;
    std::string tuning;
    unsigned sequence = 0;
    int active = 0;
    int duration = 0;
    Clock::time_point started, deadline, finished;

    ~Run() {
        stop = true;
        join();
    }

    void join() {
        if (tuner.joinable()) tuner.join();
        for (auto& worker : workers) worker.join();
        workers.clear();
    }

    void record(int i, int queue, const Counts& delta) {
        std::lock_guard lock(guard);
        for (Counts* counters : {&managers[i], &queues[i][queue - 1]}) {
            counters->puts += delta.puts;
            counters->gets += delta.gets;
            counters->bytesPut += delta.bytesPut;
            counters->bytesGot += delta.bytesGot;
            counters->persistentPuts += delta.persistentPuts;
            counters->persistentGets += delta.persistentGets;
            counters->empty += delta.empty;
            counters->errors += delta.errors;
            counters->done += delta.done;
            counters->slowestVisitMs = std::max(counters->slowestVisitMs, delta.slowestVisitMs);
            if (!delta.error.empty()) counters->error = delta.error;
        }
    }

    void work(int i, int lane, Target target, Load load) {
        bool online = false;
        bool countedFailure = false;
        try {
            Connection connection(target);
            {
                std::lock_guard lock(guard);
                ++connected[i];
                online = true;
                states[i] = "Running";
            }
            const bool put = mode != "get";
            const bool get = mode != "put";
            const bool durable = load.persistence == MQPER_PERSISTENT;
            const bool adaptive = adaptiveWorkers.load() != 0;
            std::vector<char> buffer(get ? (adaptive ? adaptiveMaxBytes : put ? load.payload.size() : maxMessageBytes) : 0);
            const auto spacing = load.rate ? 1000000us * load.connections / load.rate : 0us;
            auto next = Clock::now();
            auto running = [&] { return !stop && Clock::now() < deadline; };
            while (running()) {
                if (adaptive && lane >= adaptiveWorkers.load()) {
                    std::this_thread::sleep_for(30ms);
                    continue;
                }
                const int first = adaptive ? 1 + nextQueue[i].fetch_add(1, std::memory_order_relaxed) % 500 : 1 + lane;
                const int step = adaptive ? 500 : load.connections;
                for (int queue = first; queue <= 500 && running(); queue += step) {
                    const auto visitStarted = Clock::now();
                    Counts result;
                    bool disconnected = false;
                    try {
                        auto handle = connection.open(queueName(queue), put, get);
                        if (adaptive && !durable) load.payload.resize(adaptiveBytes.load(), ' ');
                        for (int message = 0; message < load.batch && running() &&
                             (!adaptive || lane < adaptiveWorkers.load()); ++message) {
                            std::optional<Tag> messageId;
                            if (put) {
                                messageId = connection.put(handle, tag, load.payload, load.persistence);
                                result.puts++;
                                result.bytesPut += load.payload.size();
                                result.persistentPuts += durable;
                            }
                            if (get) {
                                if (auto size = connection.get(handle, tag, buffer, messageId)) {
                                    result.gets++;
                                    result.bytesGot += *size;
                                    result.persistentGets += durable;
                                } else {
                                    result.empty++;
                                    if (!put) break;
                                }
                            }
                            record(i, queue, result);
                            result = {};
                        }
                    } catch (const MQError& error) {
                        result.errors++;
                        result.error = error.what();
                        disconnected = error.reason == MQRC_CONNECTION_BROKEN ||
                                       error.reason == MQRC_Q_MGR_NOT_AVAILABLE ||
                                       error.reason == MQRC_Q_MGR_QUIESCING;
                    }
                    result.done = 1;
                    result.slowestVisitMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                        Clock::now() - visitStarted).count();
                    record(i, queue, result);
                    if (disconnected) {
                        countedFailure = true;
                        throw std::runtime_error(result.error);
                    }
                    next = std::max(next + spacing, Clock::now());
                    while (running() && Clock::now() < next) {
                        std::this_thread::sleep_until(std::min({next, deadline, Clock::now() + 30ms}));
                    }
                }
            }
        } catch (const std::exception& error) {
            std::lock_guard lock(guard);
            ++failures[i];
            if (!countedFailure) managers[i].errors++;
            managers[i].error = error.what();
        }
        std::lock_guard lock(guard);
        if (online) --connected[i];
        if (--remaining[i] == 0) {
            if (stop) states[i] = "Stopped";
            else if (failures[i] == configured[i]) states[i] = "Failed";
            else states[i] = managers[i].errors ? "Completed with errors" : "Complete";
        } else if (connected[i] == 0) states[i] = "Connecting";
        if (--active == 0) finished = Clock::now();
    }

    void startLanes(const std::array<Target, 2>& targets, const Load& load, const Load& durable, int limit) {
        for (int i = 0; i < 2; ++i) {
            if (targets[i].manager.empty()) continue;
            if (!configured[i]) states[i] = "Connecting";
            while (configured[i] < limit) {
                const int lane = configured[i]++;
                try {
                    const bool durableLane = persistent && lane < 32 && lane % 8 == 0;
                    workers.emplace_back(&Run::work, this, i, lane, targets[i], durableLane ? durable : load);
                    ++active;
                    ++remaining[i];
                } catch (...) {
                    ++failures[i];
                    managers[i].errors++;
                    managers[i].error = "Could not start the workload worker";
                }
            }
            if (!remaining[i]) states[i] = "Failed";
        }
    }

    void tune(std::array<Target, 2> targets, Load load, Load durable) {
        auto previousCpu = hostCpu();
        uint64_t previousErrors = 0;
        std::array<uint64_t, 2> previousGets{};
        bool growPayload = false;
        auto next = Clock::now() + adaptiveWindow;
        while (!stop && Clock::now() < deadline) {
            if (Clock::now() < next) {
                std::this_thread::sleep_until(std::min({next, deadline, Clock::now() + 100ms}));
                continue;
            }
            next = Clock::now() + adaptiveWindow;
            auto currentCpu = hostCpu();
            std::lock_guard lock(guard);
            if (!active || stop || Clock::now() >= deadline) return;
            const auto errors = managers[0].errors + managers[1].errors;
            bool stalled = false;
            for (int i = 0; i < 2; ++i) {
                if (targets[i].manager.empty()) continue;
                stalled |= managers[i].gets == previousGets[i];
                previousGets[i] = managers[i].gets;
            }
            cpuPercent = currentCpu.total > previousCpu.total && previousCpu.total &&
                         currentCpu.idle >= previousCpu.idle
                ? std::clamp(100.0 * (1.0 - double(currentCpu.idle - previousCpu.idle) /
                                     double(currentCpu.total - previousCpu.total)), 0.0, 100.0)
                : -1;
            previousCpu = currentCpu;
            const int count = adaptiveWorkers.load();
            const int size = adaptiveBytes.load();
            if (errors > previousErrors || stalled) {
                adaptiveWorkers = std::max(adaptiveMinWorkers, count - 8);
                adaptiveBytes = std::max(adaptiveMinBytes, size / 2);
                tuning = errors > previousErrors ? "Backing off after MQ errors" : "Backing off after a manager stopped completing gets";
            } else if (cpuPercent < 0) {
                tuning = "Holding because CPU sampling is unavailable";
            } else if (cpuPercent >= 95) {
                tuning = "Holding near host CPU saturation";
            } else if ((growPayload || count == adaptiveMaxWorkers) && size < adaptiveMaxBytes) {
                adaptiveBytes = std::min(adaptiveMaxBytes, size * 2);
                growPayload = false;
                tuning = "Increasing message size";
            } else if (count < adaptiveMaxWorkers) {
                const int target = std::min(adaptiveMaxWorkers, count + 8);
                adaptiveWorkers = target;
                startLanes(targets, load, durable, target);
                growPayload = true;
                tuning = "Increasing concurrent workers";
            } else {
                tuning = "Holding at configured worker and size bounds";
            }
            previousErrors = errors;
        }
    }

    void start(const Fields& form) {
        {
            std::lock_guard lock(guard);
            if (active) throw std::runtime_error("Stop the current run before starting a new one");
        }
        join();
        auto requestedMode = form.at("mode");
        if (requestedMode != "roundtrip" && requestedMode != "put" && requestedMode != "get") {
            throw std::runtime_error("Choose a valid operation");
        }
        const auto selection = form.at("target");
        if (selection != "both" && selection != "0" && selection != "1") {
            throw std::runtime_error("Choose a queue manager");
        }
        const int seconds = number(form.at("seconds"), 1, maxRunSeconds);
        auto preset = profiles.find(form.at("profile"));
        if (preset == profiles.end()) throw std::runtime_error("Choose a valid load profile");
        const auto [rate, batch, bytes, connections] = preset->second;
        const bool extreme = preset->first == "extreme" && requestedMode == "roundtrip";
        const bool mixed = extreme && form.contains("diskLoad") && form.at("diskLoad") == "1";
        Load load{rate, batch, connections, "MQ Traffic Console test message"};
        load.payload.resize(bytes, ' ');
        if (extreme) load.persistence = MQPER_NOT_PERSISTENT;
        Load durable = load;
        if (mixed) {
            durable.persistence = MQPER_PERSISTENT;
            durable.payload.resize(extremePersistentBytes, ' ');
            durable.batch = 4;
        }
        auto id = workloadTag(form.at("tag"), requestedMode == "get");
        // Keep the MQ endpoints on the local Docker host. Only the published port is editable.
        std::array<Target, 2> targets;
        const char* shared = std::getenv("MQ_TRAFFIC_PASSWORD");
        const std::string password = shared ? shared : "";
        for (int i = 0; i < 2; ++i) {
            if (selection != "both" && selection != std::to_string(i)) continue;
            auto suffix = std::to_string(i);
            auto port = number(form.at("port" + suffix), 1, 65535);
            auto channel = form.at("channel" + suffix);
            auto user = form.at("user" + suffix);
            if (channel.empty() || channel.size() > MQ_CHANNEL_NAME_LENGTH || user.size() > 1024) {
                throw std::runtime_error("Check MQ channel and username");
            }
            if (!user.empty() && (password.empty() || password.size() > 1024))
                throw std::runtime_error("Set MQ_TRAFFIC_PASSWORD before starting the app");
            targets[i] = {"QM" + std::to_string(i + 1), "127.0.0.1(" + std::to_string(port) + ")",
                          std::move(channel), user, user.empty() ? "" : password};
        }

        std::lock_guard lock(guard);
        queues = {};
        managers = {};
        remaining = {};
        connected = {};
        configured = {};
        failures = {};
        states = {"Idle", "Idle"};
        for (auto& cursor : nextQueue) cursor = 0;
        stop = false;
        mode = requestedMode;
        profileName = preset->first;
        persistent = mixed;
        adaptiveWorkers = extreme ? adaptiveMinWorkers : 0;
        adaptiveBytes = extreme ? adaptiveMinBytes : 0;
        cpuPercent = -1;
        tuning = extreme ? "Starting baseline window" : "";
        tag = id;
        sequence++;
        duration = seconds;
        started = Clock::now();
        deadline = started + std::chrono::seconds(duration);
        finished = started;
        workers.reserve(2 * (extreme ? adaptiveMaxWorkers : connections));
        startLanes(targets, load, durable, extreme ? adaptiveMinWorkers : connections);
        if (extreme && active) {
            try {
                tuner = std::thread(&Run::tune, this, std::move(targets), std::move(load), std::move(durable));
            } catch (const std::system_error&) {
                tuning = "Controller unavailable. Holding baseline.";
            }
        }
    }

    static void counters(std::ostream& out, const Counts& count) {
        out << "\"puts\":" << count.puts << ",\"gets\":" << count.gets << ",\"empty\":" << count.empty
            << ",\"bytesPut\":" << count.bytesPut << ",\"bytesGot\":" << count.bytesGot
            << ",\"persistentPuts\":" << count.persistentPuts << ",\"persistentGets\":" << count.persistentGets
            << ",\"slowestVisitMs\":" << count.slowestVisitMs
            << ",\"errors\":" << count.errors << ",\"done\":" << count.done << ",\"error\":" << json(count.error);
    }

    std::string status(bool details) {
        std::lock_guard lock(guard);
        std::ostringstream out;
        out << "{\"run\":" << sequence << ",\"mode\":" << json(mode)
            << ",\"profile\":" << json(profileName) << ",\"persistent\":"
            << (persistent ? "true" : "false") << ",\"tag\":"
            << json(sequence ? hex(tag) : "") << ",\"active\":" << active << ",\"stopping\":"
            << (stop && active ? "true" : "false")
            << ",\"adaptiveWorkers\":" << adaptiveWorkers.load()
            << ",\"adaptivePayloadBytes\":" << adaptiveBytes.load()
            << ",\"hostCpuPercent\":" << cpuPercent << ",\"tuning\":" << json(tuning)
            << ",\"duration\":" << duration << ",\"elapsed\":"
            << (sequence ? std::chrono::duration_cast<std::chrono::seconds>(
                               (active ? Clock::now() : finished) - started).count() : 0)
            << ",\"managers\":[";
        for (int i = 0; i < 2; ++i) {
            if (i) out << ',';
            out << "{\"name\":\"QM" << i + 1 << "\",\"state\":" << json(states[i])
                << ",\"connections\":" << connected[i] << ",\"configuredConnections\":" << configured[i] << ',';
            counters(out, managers[i]);
            if (details) {
                out << ",\"queues\":[";
                for (int q = 0; q < 500; ++q) {
                    if (q) out << ',';
                    out << "{\"name\":" << json(queueName(q + 1)) << ',';
                    counters(out, queues[i][q]);
                    out << '}';
                }
                out << ']';
            }
            out << '}';
        }
        out << "]}";
        return out.str();
    }
};

std::string decode(const std::string& encoded) {
    std::string result;
    for (size_t i = 0; i < encoded.size(); ++i) {
        if (encoded[i] == '+') result.push_back(' ');
        else if (encoded[i] == '%') {
            unsigned char byte;
            if (encoded.size() - i < 3) throw std::runtime_error("Invalid form encoding");
            const char* start = encoded.data() + i + 1;
            auto [end, error] = std::from_chars(start, start + 2, byte, 16);
            if (error != std::errc{} || end != start + 2 || byte == 0) {
                throw std::runtime_error("Invalid form encoding");
            }
            result.push_back(static_cast<char>(byte));
            i += 2;
        } else result.push_back(encoded[i]);
    }
    return result;
}

Fields formFields(const std::string& body) {
    Fields fields;
    std::istringstream input(body);
    for (std::string pair; std::getline(input, pair, '&');) {
        auto equals = pair.find('=');
        if (equals == std::string::npos) throw std::runtime_error("Invalid form field");
        if (!fields.emplace(decode(pair.substr(0, equals)), decode(pair.substr(equals + 1))).second) {
            throw std::runtime_error("Duplicate form field");
        }
    }
    return fields;
}

struct Request {
    std::string method, path, body;
    Fields headers;
};

Request receive(int socket) {
    std::string data;
    char buffer[8192];
    auto deadline = std::chrono::steady_clock::now() + 2s;
    auto read = [&] {
        if (std::chrono::steady_clock::now() >= deadline) throw std::runtime_error("Request timed out");
        auto bytes = recv(socket, buffer, sizeof(buffer), 0);
        if (bytes <= 0) throw std::runtime_error("Incomplete request");
        data.append(buffer, bytes);
    };
    size_t end;
    while ((end = data.find("\r\n\r\n")) == std::string::npos) {
        if (data.size() >= 8192) throw std::runtime_error("Request headers are too large");
        read();
    }
    if (end >= 8192) throw std::runtime_error("Request headers are too large");
    Request request;
    std::istringstream headers(data.substr(0, end));
    std::string line, version;
    std::getline(headers, line);
    std::istringstream start(line);
    if (!(start >> request.method >> request.path >> version) || version != "HTTP/1.1") {
        throw std::runtime_error("Expected an HTTP/1.1 request");
    }
    while (std::getline(headers, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        auto colon = line.find(':');
        if (colon == std::string::npos) throw std::runtime_error("Invalid header");
        auto key = line.substr(0, colon);
        for (char& c : key) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
        auto value = line.find_first_not_of(" \t", colon + 1);
        if (!request.headers.emplace(key, value == std::string::npos ? "" : line.substr(value)).second) {
            throw std::runtime_error("Duplicate request header");
        }
    }
    if (request.headers.contains("transfer-encoding")) throw std::runtime_error("Unsupported transfer encoding");
    size_t length = request.headers.contains("content-length")
        ? number(request.headers.at("content-length"), 0, 49152) : 0;
    while (data.size() < end + 4 + length) read();
    request.body = data.substr(end + 4, length);
    return request;
}

void reply(int socket, const std::string& status, const std::string& type, const std::string& body) {
    auto response = "HTTP/1.1 " + status + "\r\nContent-Type: " + type +
        "; charset=utf-8\r\nContent-Length: " + std::to_string(body.size()) +
        "\r\nConnection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n"
        "Content-Security-Policy: default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'\r\n\r\n" + body;
    size_t sent = 0;
    while (sent < response.size()) {
        auto bytes = send(socket, response.data() + sent, response.size() - sent, MSG_NOSIGNAL);
        if (bytes <= 0) break;
        sent += bytes;
    }
}

void serve(int socket, Run& run, const std::string& port, const Fields& assets) {
    try {
        auto request = receive(socket);
        auto host = request.headers["host"];
        if (host != "127.0.0.1:" + port && host != "localhost:" + port) {
            reply(socket, "403 Forbidden", "text/plain", "Use the local dashboard address.");
            return;
        }
        if (request.method == "GET") {
            auto asset = assets.find(request.path);
            if (asset != assets.end()) {
                auto type = request.path == "/" ? "text/html" : request.path == "/style.css" ? "text/css" : "text/javascript";
                reply(socket, "200 OK", type, asset->second);
                return;
            }
            if (request.path == "/api/status" || request.path == "/api/queues") {
                reply(socket, "200 OK", "application/json", run.status(request.path == "/api/queues"));
                return;
            }
        }
        if (request.method == "POST" && (request.path == "/api/start" || request.path == "/api/stop")) {
            if (request.headers["x-mq-traffic"] != "1" ||
                (request.headers.contains("origin") && request.headers["origin"] != "http://" + host)) {
                reply(socket, "403 Forbidden", "application/json", "{\"error\":\"Use the local dashboard.\"}");
                return;
            }
            if (request.path == "/api/start") run.start(formFields(request.body));
            else run.stop = true;
            reply(socket, "200 OK", "application/json", run.status(false));
            return;
        }
        reply(socket, "404 Not Found", "text/plain", "Route not found.");
    } catch (const std::out_of_range&) {
        reply(socket, "400 Bad Request", "application/json", "{\"error\":\"Required configuration is missing.\"}");
    } catch (const std::exception& error) {
        reply(socket, "400 Bad Request", "application/json", "{\"error\":" + json(error.what()) + "}");
    }
}

std::string readAsset(const std::string& path) {
    std::ifstream file(path);
    if (!file) throw std::runtime_error("Cannot read " + path);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

volatile sig_atomic_t shutdownRequested = 0;

void requestShutdown(int) {
    shutdownRequested = 1;
}

int main(int argc, char** argv) {
    try {
        if (argc != 2 && argc != 3) throw std::runtime_error("Usage: mq_traffic WEB_DIRECTORY [HTTP_PORT]");
        std::string port = argc == 3 ? argv[2] : "8088";
        int portNumber = number(port, 1024, 65535);
        const std::string web = argv[1];
        const Fields assets = {{"/", readAsset(web + "/index.html")},
                               {"/style.css", readAsset(web + "/style.css")},
                               {"/app.js", readAsset(web + "/app.js")}};
        int listener = socket(AF_INET, SOCK_STREAM, 0);
        if (listener < 0) throw std::runtime_error("Cannot create listening socket");
        int reuse = 1;
        setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(portNumber);
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 || listen(listener, 16) != 0) {
            throw std::runtime_error("Cannot listen on 127.0.0.1:" + port + ": " + std::strerror(errno));
        }
        Run run;
        struct sigaction action{};
        action.sa_handler = requestShutdown;
        sigemptyset(&action.sa_mask);
        sigaction(SIGINT, &action, nullptr);
        sigaction(SIGTERM, &action, nullptr);
        std::cout << "MQ Traffic console at http://127.0.0.1:" << port << std::endl;
        while (!shutdownRequested) {
            pollfd waiting{listener, POLLIN, 0};
            auto ready = poll(&waiting, 1, 250);
            if (ready < 0 && errno != EINTR) throw std::runtime_error("Listening socket poll failed");
            if (ready <= 0 || shutdownRequested) continue;
            int client = accept(listener, nullptr, nullptr);
            if (client < 0) continue;
            timeval timeout{1, 0};
            setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
            serve(client, run, port, assets);
            close(client);
        }
        close(listener);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

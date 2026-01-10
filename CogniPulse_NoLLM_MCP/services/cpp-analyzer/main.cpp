#include <cstdlib>
#include <memory>
#include <iostream>
#include <string>
#include <chrono>
#include <thread>

#include <librdkafka/rdkafkacpp.h>
#include <libpq-fe.h>

static std::string getenv_str(const char* key, const std::string& def) {
    const char* v = std::getenv(key);
    return v ? std::string(v) : def;
}

static double getenv_double(const char* key, double def) {
    const char* v = std::getenv(key);
    if (!v) return def;
    try { return std::stod(v); } catch (...) { return def; }
}

static std::string now_iso() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto t = system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return std::string(buf);
}

static void ensure_tables(PGconn* conn) {
    const char* ddl =
        "CREATE TABLE IF NOT EXISTS events ("
        "  event_id TEXT PRIMARY KEY,"
        "  device_id TEXT NOT NULL,"
        "  temperature DOUBLE PRECISION,"
        "  vibration DOUBLE PRECISION,"
        "  ts TIMESTAMPTZ NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS alerts ("
        "  alert_id TEXT PRIMARY KEY,"
        "  event_id TEXT NOT NULL,"
        "  device_id TEXT NOT NULL,"
        "  metric TEXT NOT NULL,"
        "  value DOUBLE PRECISION,"
        "  threshold DOUBLE PRECISION,"
        "  severity TEXT NOT NULL,"
        "  message TEXT,"
        "  ts TIMESTAMPTZ NOT NULL"
        ");";

    PGresult* res = PQexec(conn, ddl);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "[pg] DDL failed: " << PQerrorMessage(conn) << "\n";
    }
    PQclear(res);
}

static void pg_insert_event(PGconn* conn,
                            const std::string& event_id,
                            const std::string& device_id,
                            double temperature,
                            double vibration,
                            const std::string& ts_iso) {
    std::string q =
        "INSERT INTO events(event_id, device_id, temperature, vibration, ts) VALUES("
        "'" + event_id + "',"
        "'" + device_id + "',"
        + std::to_string(temperature) + ","
        + std::to_string(vibration) + ","
        "'" + ts_iso + "'"
        ") ON CONFLICT (event_id) DO NOTHING;";

    PGresult* res = PQexec(conn, q.c_str());
    // If this fails, Postgres error will show up in the server logs; keeping analyzer hot path simple.
    PQclear(res);
}

static void pg_insert_alert(PGconn* conn,
                            const std::string& alert_id,
                            const std::string& event_id,
                            const std::string& device_id,
                            const std::string& metric,
                            double value,
                            double threshold,
                            const std::string& severity,
                            const std::string& message,
                            const std::string& ts_iso) {
    std::string q =
        "INSERT INTO alerts(alert_id, event_id, device_id, metric, value, threshold, severity, message, ts) VALUES("
        "'" + alert_id + "',"
        "'" + event_id + "',"
        "'" + device_id + "',"
        "'" + metric + "',"
        + std::to_string(value) + ","
        + std::to_string(threshold) + ","
        "'" + severity + "',"
        "'" + message + "',"
        "'" + ts_iso + "'"
        ") ON CONFLICT (alert_id) DO NOTHING;";

    PGresult* res = PQexec(conn, q.c_str());
    PQclear(res);
}

static std::unique_ptr<RdKafka::KafkaConsumer> make_consumer(const std::string& brokers,
                                                             const std::string& group_id) {
    std::string errstr;
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("group.id", group_id, errstr);
    conf->set("enable.auto.commit", "true", errstr);
    conf->set("auto.offset.reset", "earliest", errstr);

    auto* c = RdKafka::KafkaConsumer::create(conf, errstr);
    delete conf;

    if (!c) throw std::runtime_error("Failed to create consumer: " + errstr);
    return std::unique_ptr<RdKafka::KafkaConsumer>(c);
}

static std::unique_ptr<RdKafka::Producer> make_producer(const std::string& brokers) {
    std::string errstr;
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    conf->set("bootstrap.servers", brokers, errstr);

    auto* p = RdKafka::Producer::create(conf, errstr);
    delete conf;

    if (!p) throw std::runtime_error("Failed to create producer: " + errstr);
    return std::unique_ptr<RdKafka::Producer>(p);
}

int main() {
    const std::string brokers      = getenv_str("KAFKA_BROKERS", "localhost:9092");
    const std::string events_topic = getenv_str("EVENTS_TOPIC", "cogni.events");
    const std::string alerts_topic = getenv_str("ALERTS_TOPIC", "cogni.alerts");

    const std::string pg_dsn = getenv_str(
        "POSTGRES_DSN",
        "host=localhost port=5432 dbname=cognipulse user=cognipulse password=cognipulse"
    );

    const double temp_threshold = getenv_double("TEMP_THRESHOLD", 80.0);
    const double vib_threshold  = getenv_double("VIB_THRESHOLD", 8.0);

    std::cout << "[analyzer] brokers=" << brokers
              << " events=" << events_topic
              << " alerts=" << alerts_topic << "\n";
    std::cout << "[analyzer] thresholds: temp=" << temp_threshold
              << " vib=" << vib_threshold << "\n";

    PGconn* conn = PQconnectdb(pg_dsn.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "[pg] connection failed: " << PQerrorMessage(conn) << "\n";
        return 1;
    }
    ensure_tables(conn);

    auto consumer = make_consumer(brokers, "cognipulse-cpp-analyzer");
    auto producer = make_producer(brokers);

    consumer->subscribe({events_topic});

    while (true) {
        std::unique_ptr<RdKafka::Message> msg(consumer->consume(1000));
        if (!msg) continue;

        if (msg->err() == RdKafka::ERR__TIMED_OUT) continue;

        if (msg->err() != RdKafka::ERR_NO_ERROR) {
            std::cerr << "[kafka] consume error: " << msg->errstr() << "\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        std::string payload(static_cast<const char*>(msg->payload()), msg->len());

        // Very lightweight JSON field extraction (MVP). Safe enough for known simulator payload shape.
        auto get_field = [&](const std::string& key) -> std::string {
            auto pos = payload.find("\"" + key + "\"");
            if (pos == std::string::npos) return "";
            auto colon = payload.find(":", pos);
            if (colon == std::string::npos) return "";
            auto start = payload.find_first_not_of(" \t\n", colon + 1);
            if (start == std::string::npos) return "";

            if (payload[start] == '"') {
                auto end = payload.find('"', start + 1);
                if (end == std::string::npos) return "";
                return payload.substr(start + 1, end - (start + 1));
            } else {
                auto end = payload.find_first_of(",}", start);
                if (end == std::string::npos) end = payload.size();
                return payload.substr(start, end - start);
            }
        };

        std::string event_id  = get_field("event_id");
        std::string device_id = get_field("device_id");

        double temperature = 0.0, vibration = 0.0;
        try { temperature = std::stod(get_field("temperature")); } catch (...) {}
        try { vibration   = std::stod(get_field("vibration")); }   catch (...) {}

        std::string ts = get_field("ts");
        if (ts.empty()) ts = now_iso();

        if (event_id.empty()) {
            // If producer didn’t supply an id, create one so Postgres primary key isn’t empty.
            event_id = "evt-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        }
        if (device_id.empty()) device_id = "unknown";

        // Persist raw metrics
        pg_insert_event(conn, event_id, device_id, temperature, vibration, ts);

        // Helper to publish an alert (and also store it in Postgres)
        auto publish_alert = [&](const std::string& metric,
                                 double value,
                                 double threshold,
                                 const std::string& severity,
                                 const std::string& message) {
            std::string alert_id =
                "alrt-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
            std::string ats = now_iso();

            pg_insert_alert(conn, alert_id, event_id, device_id, metric, value, threshold, severity, message, ats);

            std::string alert_json =
                "{"
                "\"alert_id\":\"" + alert_id + "\","
                "\"event_id\":\"" + event_id + "\","
                "\"device_id\":\"" + device_id + "\","
                "\"metric\":\"" + metric + "\","
                "\"value\":" + std::to_string(value) + ","
                "\"threshold\":" + std::to_string(threshold) + ","
                "\"severity\":\"" + severity + "\","
                "\"message\":\"" + message + "\","
                "\"ts\":\"" + ats + "\""
                "}";

            auto err = producer->produce(
                alerts_topic,
                RdKafka::Topic::PARTITION_UA,
                RdKafka::Producer::RK_MSG_COPY,
                const_cast<char*>(alert_json.data()),
                alert_json.size(),
                nullptr, 0, 0, nullptr
            );

            if (err != RdKafka::ERR_NO_ERROR) {
                std::cerr << "[kafka] produce alert error: " << RdKafka::err2str(err) << "\n";
            } else {
                producer->poll(0);
                std::cout << "[alert] " << alert_json << "\n";
            }
        };

        // Anomaly checks
        if (temperature >= temp_threshold) {
            publish_alert("temperature", temperature, temp_threshold, "high",
                          "Temperature exceeded safe threshold.");
        }
        if (vibration >= vib_threshold) {
            publish_alert("vibration", vibration, vib_threshold, "high",
                          "Vibration exceeded safe threshold.");
        }
    }

    PQfinish(conn);
    return 0;
}

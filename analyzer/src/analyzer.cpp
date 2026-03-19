#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <csignal>

using json = nlohmann::json;

static bool run = true;

void signal_handler(int sig) {
    run = false;
}

int main() {
    std::signal(SIGINT, signal_handler);

    std::string brokers = "redpanda:9092";
    std::string errstr;

    // -------------------------
    // Consumer Configuration
    // -------------------------
    RdKafka::Conf* consumer_conf =
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    consumer_conf->set("bootstrap.servers", brokers, errstr);
    consumer_conf->set("group.id", "analyzer-group", errstr);
    consumer_conf->set("auto.offset.reset", "earliest", errstr);

    RdKafka::KafkaConsumer* consumer =
        RdKafka::KafkaConsumer::create(consumer_conf, errstr);

    if (!consumer) {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        return 1;
    }

    consumer->subscribe({"cogni.events"});

    // -------------------------
    // Producer Configuration
    // -------------------------
    RdKafka::Conf* producer_conf =
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    producer_conf->set("bootstrap.servers", brokers, errstr);

    RdKafka::Producer* producer =
        RdKafka::Producer::create(producer_conf, errstr);

    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return 1;
    }

    std::cout << "Analyzer started. Waiting for events..." << std::endl;

    // -------------------------
    // Main Processing Loop
    // -------------------------
    while (run) {
        RdKafka::Message* msg = consumer->consume(1000);

        if (msg->err() == RdKafka::ERR_NO_ERROR) {

            std::string payload(
                static_cast<const char*>(msg->payload()),
                msg->len()
            );

            std::cout << "Consumed event: " << payload << std::endl;

            try {
                json event = json::parse(payload);
                std::string metric_name = event["metric_name"];
                double metric_value     = event["metric_value"];
                std::string device_id   = event["device_id"];
                long long timestamp     = event["timestamp"];

                std::string alert_type, severity, reason;
                bool should_alert = false;

                if (metric_name == "temperature" && metric_value > 66) {
                    alert_type    = "OVERHEAT";
                    severity      = metric_value > 100 ? "CRITICAL" : "WARNING";
                    reason        = "Temperature exceeded threshold";
                    should_alert  = true;
                } else if (metric_name == "vibration" && metric_value > 0.08) {
                    alert_type    = "HIGH_VIBRATION";
                    severity      = metric_value > 0.15 ? "CRITICAL" : "WARNING";
                    reason        = "Vibration exceeded safe limit";
                    should_alert  = true;
                } else if (metric_name == "pressure" && metric_value < 2.5) {
                    alert_type    = "LOW_PRESSURE";
                    severity      = metric_value < 1.5 ? "CRITICAL" : "WARNING";
                    reason        = "Pressure dropped below safe threshold";
                    should_alert  = true;
                }

                if (should_alert) {
                    json alert = {
                        {"device_id",  device_id},
                        {"timestamp",  timestamp},
                        {"alert_type", alert_type},
                        {"severity",   severity},
                        {"reason",     reason},
                        {"metric_name",  metric_name},
                        {"metric_value", metric_value}
                    };

                    std::string alert_payload = alert.dump();

                    RdKafka::ErrorCode resp = producer->produce(
                        "cogni.alerts",
                        RdKafka::Topic::PARTITION_UA,
                        RdKafka::Producer::RK_MSG_COPY,
                        const_cast<char*>(alert_payload.c_str()),
                        alert_payload.size(),
                        nullptr,
                        0,
                        0,
                        nullptr
                    );

                    if (resp != RdKafka::ERR_NO_ERROR) {
                        std::cerr << "Failed to produce alert: "
                                  << RdKafka::err2str(resp)
                                  << std::endl;
                    } else {
                        std::cout << "🚨 Produced alert: "
                                  << alert_payload
                                  << std::endl;
                    }

                    producer->poll(0);
                }

            } catch (std::exception& e) {
                std::cerr << "JSON parse error: "
                          << e.what()
                          << std::endl;
            }

        } else if (msg->err() != RdKafka::ERR__TIMED_OUT) {
            std::cerr << "Consumer error: "
                      << msg->errstr()
                      << std::endl;
        }

        delete msg;
    }

    // -------------------------
    // Shutdown
    // -------------------------
    std::cout << "Shutting down analyzer..." << std::endl;

    consumer->close();
    producer->flush(5000);

    delete consumer;
    delete producer;

    RdKafka::wait_destroyed(5000);

    return 0;
}

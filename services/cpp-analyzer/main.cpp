#include <cstdlib>
#include <iostream>
#include <string>
#include <chrono>
#include <memory>

#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

static std::string getenv_or(const char* key, const std::string& fallback) {
  const char* v = std::getenv(key);
  return v ? std::string(v) : fallback;
}

static double now_s() {
  using namespace std::chrono;
  return (double)duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() / 1000.0;
}

class DeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
  void dr_cb(RdKafka::Message &message) override {
    if (message.err() != RdKafka::ERR_NO_ERROR) {
      std::cerr << "[cpp-analyzer] delivery failed: " << message.errstr() << "\n";
    }
  }
};

int main() {
  const std::string BOOTSTRAP = getenv_or("BOOTSTRAP", "redpanda:9092");
  const std::string IN_TOPIC  = getenv_or("IN_TOPIC", "cogni.telemetry");
  const std::string OUT_TOPIC = getenv_or("OUT_TOPIC", "cogni.alerts");

  const double WARNING_TEMP  = 85.0;
  const double CRITICAL_TEMP = 100.0;
  const double WARNING_VIB   = 2.0;

  std::string errstr;

  auto cconf = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  cconf->set("bootstrap.servers", BOOTSTRAP, errstr);
  cconf->set("group.id", "cognipulse-cpp-analyzer", errstr);
  cconf->set("auto.offset.reset", "latest", errstr);

  auto consumer = std::unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(cconf.get(), errstr));
  if (!consumer) {
    std::cerr << "[cpp-analyzer] failed to create consumer: " << errstr << "\n";
    return 1;
  }
  if (consumer->subscribe({IN_TOPIC}) != RdKafka::ERR_NO_ERROR) {
    std::cerr << "[cpp-analyzer] subscribe failed\n";
    return 1;
  }

  DeliveryReportCb dr_cb;
  auto pconf = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  pconf->set("bootstrap.servers", BOOTSTRAP, errstr);
  pconf->set("dr_cb", &dr_cb, errstr);

  auto producer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(pconf.get(), errstr));
  if (!producer) {
    std::cerr << "[cpp-analyzer] failed to create producer: " << errstr << "\n";
    return 1;
  }

  std::cout << "[cpp-analyzer] consume=" << IN_TOPIC << " produce=" << OUT_TOPIC << "\n";

  while (true) {
    std::unique_ptr<RdKafka::Message> msg(consumer->consume(1000));
    if (msg->err() == RdKafka::ERR__TIMED_OUT) continue;
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      std::cerr << "[cpp-analyzer] consume error: " << msg->errstr() << "\n";
      continue;
    }

    std::string payload((const char*)msg->payload(), msg->len());

    json event;
    try { event = json::parse(payload); }
    catch (...) { std::cerr << "[cpp-analyzer] invalid json\n"; continue; }

    const std::string device = event.value("device_id", "unknown");
    const double temp = event.value("temperature_c", 0.0);
    const double vib  = event.value("vibration", 0.0);
    const double ts   = event.value("ts", 0.0);

    std::string severity, reason;
    if (temp >= CRITICAL_TEMP) { severity = "critical"; reason = "temperature >= 100C"; }
    else if (temp >= WARNING_TEMP) { severity = "warning"; reason = "temperature >= 85C"; }
    else if (vib >= WARNING_VIB) { severity = "warning"; reason = "vibration >= 2.0"; }
    else { continue; }

    json alert = {
      {"device_id", device},
      {"temperature_c", temp},
      {"vibration", vib},
      {"severity", severity},
      {"reason", reason},
      {"source_ts", ts},
      {"alert_ts", now_s()}
    };

    std::string out = alert.dump();

    auto perr = producer->produce(
      OUT_TOPIC, RdKafka::Topic::PARTITION_UA,
      RdKafka::Producer::RK_MSG_COPY,
      (void*)out.data(), out.size(),
      device.c_str(), device.size(),
      0, nullptr
    );

    if (perr != RdKafka::ERR_NO_ERROR)
      std::cerr << "[cpp-analyzer] produce failed: " << RdKafka::err2str(perr) << "\n";
    else
      std::cout << "[cpp-analyzer] ALERT " << out << "\n";

    producer->poll(0);
  }
}

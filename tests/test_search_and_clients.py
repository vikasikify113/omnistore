"""What we send to OpenSearch and Kafka. These need no running servers."""
import json

from confluent_kafka import Producer

from omnistore.bus import KafkaBus, client_config
from omnistore.config import Settings
from omnistore.search import SearchClient, build_search_body


# ---------------------------------------------------------------- query builder
def test_query_is_fuzzy_and_prefix_aware():
    body = build_search_body("bluetoth")
    should = body["query"]["bool"]["must"][0]["bool"]["should"]
    mm = next(s["multi_match"] for s in should if "multi_match" in s)
    assert mm["fuzziness"] == "AUTO" and mm["query"] == "bluetoth"
    assert "name^3" in mm["fields"]
    assert any("match_phrase_prefix" in s for s in should)  # "blueto" works while typing


def test_filters_for_category_price_city_and_stock():
    body = build_search_body("earphones", category="Electronics", city="Bengaluru",
                             min_price=500, max_price=2000, in_stock=True, page=3, size=10)
    f = body["query"]["bool"]["filter"]
    assert {"term": {"city": "Bengaluru"}} in f and {"term": {"category": "Electronics"}} in f
    assert {"range": {"price": {"gte": 500, "lte": 2000}}} in f
    assert {"range": {"stock": {"gte": 1}}} in f
    assert body["from"] == 20 and body["size"] == 10


def test_empty_query_browses_everything_in_a_stable_order():
    body = build_search_body("  ", city="Pune")
    assert body["query"]["bool"]["must"] == [{"match_all": {}}]
    assert body["sort"][0] == {"category": "asc"}


# ---------------------------------------------------------------- OpenSearch wire format
class Recorder:
    def __init__(self, reply):
        self.calls, self.reply = [], reply

    def __call__(self, method, url, params=None, body=None, headers=None, **kw):
        self.calls.append({"method": method, "url": url, "params": params or {}, "body": body})
        return self.reply(method, url) if callable(self.reply) else self.reply


def make_client(reply):
    sc = SearchClient("https://avnadmin:p%40ss@os.example.com:12345", "omnistore-listings")
    rec = Recorder(reply)
    sc.client.transport.perform_request = rec
    return sc, rec


def test_url_with_credentials_is_parsed():
    sc = SearchClient("https://avnadmin:p%40ss@os.example.com:12345", "idx")
    assert sc.client.transport.hosts[0]["host"] == "os.example.com"
    assert sc.client.transport.hosts[0]["port"] == 12345


def test_index_listing_sends_external_gte_version():
    sc, rec = make_client({"result": "created"})
    doc = {"listing_id": "1:Bengaluru", "name": "x"}
    assert sc.index_listing(doc, 7) == "indexed"
    call = rec.calls[0]
    assert call["method"] == "PUT" and "omnistore-listings/_doc/1" in call["url"]
    p = {k: (v.decode() if isinstance(v, bytes) else str(v)) for k, v in call["params"].items()}
    assert p["version"] == "7" and p["version_type"] == "external_gte" and p["refresh"] == "wait_for"


def test_stale_version_conflict_is_reported_not_raised():
    from opensearchpy.exceptions import ConflictError

    def reply(method, url):
        raise ConflictError(409, "version_conflict_engine_exception", {})

    sc, _ = make_client(reply)
    assert sc.index_listing({"listing_id": "1:Pune"}, 1) == "stale"


def test_search_posts_the_built_body_and_maps_hits():
    hits = {"took": 4, "hits": {"total": {"value": 1}, "hits": [{"_score": 2.5, "_source": {"name": "Bluetooth Earphones"}}]}}
    sc, rec = make_client(hits)
    out = sc.search("bluetoth", city="Pune")
    assert rec.calls[0]["url"].endswith("/omnistore-listings/_search")
    sent = rec.calls[0]["body"]
    sent = json.loads(sent) if isinstance(sent, (str, bytes)) else sent
    assert {"term": {"city": "Pune"}} in sent["query"]["bool"]["filter"]
    assert out["total"] == 1 and out["took_ms"] == 4 and out["items"][0]["score"] == 2.5


def test_bulk_index_carries_versions():
    sc, rec = make_client({"errors": False, "items": [{"index": {"status": 201}}]})
    result = sc.bulk_index([({"listing_id": "1:Pune", "name": "x"}, 4)])
    body = rec.calls[0]["body"]
    body = body if isinstance(body, str) else (body.decode() if isinstance(body, bytes) else "\n".join(body))
    assert '"version": 4' in body.replace('":4', '": 4') or '"version":4' in body
    assert "external_gte" in body and result["indexed"] == 1


# ---------------------------------------------------------------- Kafka configuration
def settings_for(**kw):
    s = Settings.from_env()
    for k, v in kw.items():
        setattr(s, k, v)
    return s


def test_ssl_client_certificate_config_for_aiven():
    conf = client_config(settings_for(kafka_bootstrap="k.example.com:1", kafka_security_protocol="SSL",
                                      kafka_ssl_ca="certs/ca.pem", kafka_ssl_cert="certs/service.cert",
                                      kafka_ssl_key="certs/service.key"))
    assert conf["security.protocol"] == "SSL"
    assert conf["ssl.ca.location"] == "certs/ca.pem"
    assert conf["ssl.certificate.location"] == "certs/service.cert" and conf["ssl.key.location"] == "certs/service.key"


def test_sasl_config_is_accepted_by_librdkafka():
    conf = client_config(settings_for(kafka_bootstrap="k.example.com:1", kafka_security_protocol="SASL_SSL",
                                      kafka_sasl_mechanism="SCRAM-SHA-256", kafka_sasl_username="avnadmin",
                                      kafka_sasl_password="pw", kafka_ssl_ca=""))
    Producer(conf)  # raises if any setting name or value is invalid
    assert conf["sasl.mechanisms"] == "SCRAM-SHA-256" and conf["sasl.username"] == "avnadmin"


def test_unreachable_broker_reports_failure_instead_of_hanging():
    s = settings_for(kafka_bootstrap="127.0.0.1:9", kafka_security_protocol="PLAINTEXT", kafka_message_timeout_ms=1500)
    results = KafkaBus(s).publish_many([("t", "k", b"{}")])
    assert results[0].ok is False and results[0].error

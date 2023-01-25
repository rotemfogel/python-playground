import json
from urllib.parse import parse_qsl, unquote, urlparse


class Connection:
    def __init__(
        self,
        conn_id=None,
        conn_type=None,
        host=None,
        login=None,
        password=None,
        schema=None,
        port=None,
        extra=None,
        uri=None,
    ):
        self.conn_id = conn_id
        if uri:
            self.parse_from_uri(uri)
        else:
            self.conn_type = conn_type
            self.host = host
            self.login = login
            self.password = password
            self.schema = schema
            self.port = port
            self.extra = extra

    def parse_from_uri(self, uri):
        uri_parts = urlparse(uri)
        conn_type = uri_parts.scheme
        if conn_type == "postgresql":
            conn_type = "postgres"
        elif "-" in conn_type:
            conn_type = conn_type.replace("-", "_")
        self.conn_type = conn_type
        self.host = parse_netloc_to_hostname(uri_parts)
        quoted_schema = uri_parts.path[1:]
        self.schema = unquote(quoted_schema) if quoted_schema else quoted_schema
        self.login = (
            unquote(uri_parts.username) if uri_parts.username else uri_parts.username
        )
        self.password = (
            unquote(uri_parts.password) if uri_parts.password else uri_parts.password
        )
        self.port = uri_parts.port
        if uri_parts.query:
            self.extra = json.dumps(
                dict(parse_qsl(uri_parts.query, keep_blank_values=True))
            )


def parse_netloc_to_hostname(uri_parts):
    hostname = unquote(uri_parts.hostname or "")
    if "/" in hostname:
        hostname = uri_parts.netloc
        if "@" in hostname:
            hostname = hostname.rsplit("@", 1)[1]
        if ":" in hostname:
            hostname = hostname.split(":", 1)[0]
        hostname = unquote(hostname)
    return hostname

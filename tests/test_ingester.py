import pytest

from alertingest.ingester import _read_confluent_wire_format_header


def test_confluent_wire_format_parsing():
    data = b"\x00\x00\x00\x00\x02"
    have = _read_confluent_wire_format_header(data)
    assert have == 2

    data = b"\x00"
    with pytest.raises(ValueError):
        # too short
        _read_confluent_wire_format_header(data)

    data = b"\x01\x00\x00\x00\x02"
    with pytest.raises(ValueError):
        # wrong magic byte
        _read_confluent_wire_format_header(data)

    # extra data is ok
    data = b"\x00\x00\x00\x00\x04\xd3"
    have = _read_confluent_wire_format_header(data)
    assert have == 4

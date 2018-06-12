from IPy import IP

from ..api import DatabaseHandler
from unittest.mock import Mock


class TestDatabaseHandlerLive:
    """
    This test for the database handler uses a live PostgreSQL database,
    as it's rather complicated to mock, and mocking would not make it
    a very useful test. Using in-memory SQLite is not an option due to
    using specific PostgreSQL features.
    """

    def test_object_writing(self, monkeypatch):
        monkeypatch.setattr('irrd.db.api.MAX_RECORDS_CACHE_BEFORE_INSERT', 1)

        rpsl_object_route_v4 = Mock(
            pk=lambda: '192.0.2.0/24,AS23456',
            rpsl_object_class='route',
            parsed_data={'mnt-by': 'MNT-WRONG', 'source': 'TEST'},
            render_rpsl_text=lambda: 'object-text',
            ip_version=lambda: 4,
            ip_first=IP('192.0.2.0'),
            ip_last=IP('192.0.2.255'),
            asn_first=23456,
            asn_last=23456,
        )

        dh = DatabaseHandler()
        dh.upsert_object(rpsl_object_route_v4)
        assert len(dh._records) == 1

        rpsl_object_route_v4.parsed_data = {'mnt-by': 'MNT-CORRECT', 'source': 'TEST'}
        dh.upsert_object(rpsl_object_route_v4)  # should trigger an immediate flush due to duplicate RPSL pk
        assert len(dh._records) == 1

        rpsl_obj_route_v6 = Mock(
            pk=lambda: '2001:db8::/64,AS23456',
            rpsl_object_class='route',
            parsed_data={'mnt-by': 'MNT-CORRECT', 'source': 'TEST'},
            render_rpsl_text=lambda: 'object-text',
            ip_version=lambda: 6,
            ip_first=IP('2001:db8::'),
            ip_last=IP('2001:db8::ffff:ffff:ffff:ffff'),
            asn_first=23456,
            asn_last=23456,
        )
        dh.upsert_object(rpsl_obj_route_v6)
        assert len(dh._records) == 0  # should have been flushed to the DB
        dh.upsert_object(rpsl_obj_route_v6)

        dh.commit()

        # TODO: validate two entries in the db - should have MNT-CORRECT

        rpsl_obj_ignored = Mock(
            pk=lambda: '2001:db8::/64,AS2914',
            rpsl_object_class='route',
            parsed_data={'mnt-by': 'MNT-CORRECT', 'source': 'TEST'},
            render_rpsl_text=lambda: 'object-text',
            ip_version=lambda: 6,
            ip_first=IP('2001:db8::'),
            ip_last=IP('2001:db8::ffff:ffff:ffff:ffff'),
            asn_first=23456,
            asn_last=23456,
        )
        dh.upsert_object(rpsl_obj_ignored)
        assert len(dh._records) == 1
        dh.upsert_object(rpsl_obj_ignored)
        assert len(dh._records) == 1
        dh.rollback()

        # TODO: validate no new entries in the DB

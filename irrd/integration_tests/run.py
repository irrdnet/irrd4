# flake8: noqa: W293
import sys
import time
import ujson

import base64
import email
import os
import signal
import socket
import sqlalchemy as sa
import subprocess
import textwrap
import yaml
from alembic import command, config
from pathlib import Path

from irrd.conf import config_init, PASSWORD_HASH_DUMMY_VALUE
from irrd.utils.rpsl_samples import (SAMPLE_MNTNER, SAMPLE_PERSON, SAMPLE_KEY_CERT, SIGNED_PERSON_UPDATE_VALID,
                                     SAMPLE_AS_SET, SAMPLE_AUT_NUM, SAMPLE_DOMAIN, SAMPLE_FILTER_SET, SAMPLE_INET_RTR,
                                     SAMPLE_INET6NUM, SAMPLE_INETNUM, SAMPLE_PEERING_SET, SAMPLE_ROLE, SAMPLE_ROUTE,
                                     SAMPLE_ROUTE_SET, SAMPLE_ROUTE6, SAMPLE_RTR_SET, SAMPLE_AS_BLOCK)
from irrd.utils.whois_client import whois_query, whois_query_irrd
from .constants import (EMAIL_SMTP_PORT, EMAIL_DISCARD_MSGS_COMMAND, EMAIL_RETURN_MSGS_COMMAND, EMAIL_SEPARATOR,
                        EMAIL_END)

IRRD_ROOT_PATH = str(Path(__file__).resolve().parents[2])
sys.path.append(IRRD_ROOT_PATH)

AS_SET_REFERRING_OTHER_SET = """as-set:         AS-TESTREF
descr:          description
members:        AS-SETTEST, AS65540
tech-c:         PERSON-TEST
admin-c:        PERSON-TEST
notify:         notify@example.com
mnt-by:         TEST-MNT
changed:        changed@example.com 20190701 # comment
source:         TEST
remarks:        remark
"""

SAMPLE_MNTNER_CLEAN = SAMPLE_MNTNER.replace('mnt-by:         OTHER1-MNT,OTHER2-MNT\n', '')
LARGE_UPDATE = '\n\n'.join([
    SAMPLE_AS_BLOCK,
    SAMPLE_AS_SET,
    SAMPLE_AUT_NUM,
    SAMPLE_AUT_NUM.replace('aut-num:        as065537', 'aut-num: as65538'),
    SAMPLE_AUT_NUM.replace('aut-num:        as065537', 'aut-num: as65539'),
    SAMPLE_AUT_NUM.replace('aut-num:        as065537', 'aut-num: as65540'),
    SAMPLE_DOMAIN,
    SAMPLE_FILTER_SET,
    SAMPLE_INET_RTR,
    SAMPLE_INET6NUM,
    SAMPLE_INETNUM,
    SAMPLE_KEY_CERT,
    SAMPLE_PEERING_SET,
    SAMPLE_PERSON.replace('PERSON-TEST', 'DUMY2-TEST'),
    SAMPLE_ROLE,
    SAMPLE_ROUTE,
    SAMPLE_ROUTE_SET,
    SAMPLE_ROUTE6,
    SAMPLE_RTR_SET,
    AS_SET_REFERRING_OTHER_SET,
])


class TestIntegration:
    """
    This integration test will start two instances of IRRd, one mirroring off the
    other, and an email server that captures all mail. It will then run a series
    of updates and queries, verify the contents of mails, the state of the
    databases, mirroring, utf-8 handling and run all basic types of queries.

    Note that this test will not be included in the default py.test discovery,
    this is intentional.
    """
    port_http1 = 6080
    port_whois1 = 6043
    port_http2 = 6081
    port_whois2 = 6044

    def test_irrd_integration(self, tmpdir):
        # IRRD_DATABASE_URL overrides the yaml config, so should be removed
        if 'IRRD_DATABASE_URL' in os.environ:
            del os.environ['IRRD_DATABASE_URL']
        # PYTHONPATH needs to contain the twisted plugin path.
        os.environ['PYTHONPATH'] = IRRD_ROOT_PATH
        os.environ['IRRD_SCHEDULER_TIMER_OVERRIDE'] = '1'
        self.tmpdir = tmpdir

        self._start_mailserver()
        self._start_irrds()

        # Attempt to load a mntner with valid auth, but broken references.
        self._submit_update(self.config_path1, SAMPLE_MNTNER + '\n\noverride: override-password')
        messages = self._retrieve_mails()
        assert len(messages) == 1
        mail_text = self._extract_message_body(messages[0])
        assert messages[0]['Subject'] == 'FAILED: my subject'
        assert messages[0]['From'] == 'from@example.com'
        assert messages[0]['To'] == 'Sasha <sasha@example.com>'
        assert '\nCreate FAILED: [mntner] TEST-MNT\n' in mail_text
        assert '\nERROR: Object PERSON-TEST referenced in field admin-c not found in database TEST - must reference one of role, person.\n' in mail_text
        assert '\nERROR: Object OTHER1-MNT referenced in field mnt-by not found in database TEST - must reference mntner.\n' in mail_text
        assert '\nERROR: Object OTHER2-MNT referenced in field mnt-by not found in database TEST - must reference mntner.\n' in mail_text
        assert 'email footer' in mail_text
        assert 'Generated by IRRd version ' in mail_text

        # Load a regular valid mntner and person into the DB, and verify
        # the contents of the result.
        self._submit_update(self.config_path1,
                            SAMPLE_MNTNER_CLEAN + '\n\n' + SAMPLE_PERSON + '\n\noverride: override-password')
        messages = self._retrieve_mails()
        assert len(messages) == 1
        mail_text = self._extract_message_body(messages[0])
        assert messages[0]['Subject'] == 'SUCCESS: my subject'
        assert messages[0]['From'] == 'from@example.com'
        assert messages[0]['To'] == 'Sasha <sasha@example.com>'
        assert '\nCreate succeeded: [mntner] TEST-MNT\n' in mail_text
        assert '\nCreate succeeded: [person] PERSON-TEST\n' in mail_text
        assert 'email footer' in mail_text
        assert 'Generated by IRRd version ' in mail_text

        # Check whether the objects can be queried from irrd #1,
        # whether the hash is masked, and whether encoding is correct.
        mntner_text = whois_query('127.0.0.1', self.port_whois1, 'TEST-MNT')
        assert 'TEST-MNT' in mntner_text
        assert PASSWORD_HASH_DUMMY_VALUE in mntner_text
        assert 'unįcöde tæst 🌈🦄' in mntner_text
        assert 'PERSON-TEST' in mntner_text

        # After three seconds, a new export should have been generated by irrd #1,
        # loaded by irrd #2, and the objects should be available in irrd #2
        time.sleep(3)
        mntner_text = whois_query('127.0.0.1', self.port_whois2, 'TEST-MNT')
        assert 'TEST-MNT' in mntner_text
        assert PASSWORD_HASH_DUMMY_VALUE in mntner_text
        assert 'unįcöde tæst 🌈🦄' in mntner_text
        assert 'PERSON-TEST' in mntner_text

        # Load a key-cert. This should cause notifications to mnt-nfy (2x).
        # Change is authenticated by valid password.
        self._submit_update(self.config_path1, SAMPLE_KEY_CERT + '\npassword: md5-password')
        messages = self._retrieve_mails()
        assert len(messages) == 3
        assert messages[0]['Subject'] == 'SUCCESS: my subject'
        assert messages[0]['From'] == 'from@example.com'
        assert messages[0]['To'] == 'Sasha <sasha@example.com>'
        assert 'Create succeeded: [key-cert] PGPKEY-80F238C6' in self._extract_message_body(messages[0])

        self._check_recipients_in_mails(messages[1:], [
            'mnt-nfy@example.net', 'mnt-nfy2@example.net'
        ])

        self._check_text_in_mails(messages[1:], [
            '\n> Message-ID: <1325754288.4989.6.camel@hostname>\n',
            '\nCreate succeeded for object below: [key-cert] PGPKEY-80F238C6:\n',
            'email footer',
            'Generated by IRRd version ',
        ])
        for message in messages[1:]:
            assert message['Subject'] == 'Notification of TEST database changes'
            assert message['From'] == 'from@example.com'

        # Use the new PGP key to make an update to PERSON-TEST. Should
        # again trigger mnt-nfy messages, and a mail to the notify address
        # of PERSON-TEST.
        self._submit_update(self.config_path1, SIGNED_PERSON_UPDATE_VALID)
        messages = self._retrieve_mails()
        assert len(messages) == 4
        mail_text = self._extract_message_body(messages[0])
        assert messages[0]['Subject'] == 'SUCCESS: my subject'
        assert messages[0]['From'] == 'from@example.com'
        assert messages[0]['To'] == 'Sasha <sasha@example.com>'
        assert '\nModify succeeded: [person] PERSON-TEST\n' in mail_text

        self._check_recipients_in_mails(messages[1:], [
            'mnt-nfy@example.net', 'mnt-nfy2@example.net', 'notify@example.com',
        ])

        self._check_text_in_mails(messages[1:], [
            '\n> Message-ID: <1325754288.4989.6.camel@hostname>\n',
            '\nModify succeeded for object below: [person] PERSON-TEST:\n',
            '\n@@ -1,4 +1,4 @@\n',
            '\nNew version of this object:\n',
        ])
        for message in messages[1:]:
            assert message['Subject'] == 'Notification of TEST database changes'
            assert message['From'] == 'from@example.com'

        # Check that the person is updated on irrd #1
        person_text = whois_query('127.0.0.1', self.port_whois1, 'PERSON-TEST')
        assert 'PERSON-TEST' in person_text
        assert 'Test person changed by PGP signed update' in person_text

        # After 2s, NRTM from irrd #2 should have picked up the change.
        time.sleep(2)
        person_text = whois_query('127.0.0.1', self.port_whois2, 'PERSON-TEST')
        assert 'PERSON-TEST' in person_text
        assert 'Test person changed by PGP signed update' in person_text

        # Submit an update back to the original person object, with an invalid
        # password and invalid override. Should trigger notification to upd-to.
        self._submit_update(self.config_path1, SAMPLE_PERSON + '\npassword: invalid\noverride: invalid\n')
        messages = self._retrieve_mails()
        assert len(messages) == 2
        mail_text = self._extract_message_body(messages[0])
        assert messages[0]['Subject'] == 'FAILED: my subject'
        assert messages[0]['From'] == 'from@example.com'
        assert messages[0]['To'] == 'Sasha <sasha@example.com>'
        assert '\nModify FAILED: [person] PERSON-TEST\n' in mail_text
        assert '\nERROR: Authorisation for person PERSON-TEST failed: must by authenticated by one of: TEST-MNT\n' in mail_text

        mail_text = self._extract_message_body(messages[1])
        assert messages[1]['Subject'] == 'Notification of TEST database changes'
        assert messages[1]['From'] == 'from@example.com'
        assert messages[1]['To'] == 'upd-to@example.net'
        assert '\nModify FAILED AUTHORISATION for object below: [person] PERSON-TEST:\n' in mail_text

        # Object should not have changed by latest update.
        person_text = whois_query('127.0.0.1', self.port_whois1, 'PERSON-TEST')
        assert 'PERSON-TEST' in person_text
        assert 'Test person changed by PGP signed update' in person_text

        # Submit a delete with a valid password for PERSON-TEST.
        # This should be rejected, because it creates a dangling reference.
        # No mail should be sent to upd-to.
        self._submit_update(self.config_path1, SAMPLE_PERSON + 'password: md5-password\ndelete: delete\n')
        messages = self._retrieve_mails()
        assert len(messages) == 1
        mail_text = self._extract_message_body(messages[0])
        assert messages[0]['Subject'] == 'FAILED: my subject'
        assert messages[0]['From'] == 'from@example.com'
        assert messages[0]['To'] == 'Sasha <sasha@example.com>'
        assert '\nDelete FAILED: [person] PERSON-TEST\n' in mail_text
        assert '\nERROR: Object PERSON-TEST to be deleted, but still referenced by mntner TEST-MNT\n' in mail_text
        assert '\nERROR: Object PERSON-TEST to be deleted, but still referenced by key-cert PGPKEY-80F238C6\n' in mail_text

        # Object should not have changed by latest update.
        person_text = whois_query('127.0.0.1', self.port_whois1, 'PERSON-TEST')
        assert 'PERSON-TEST' in person_text
        assert 'Test person changed by PGP signed update' in person_text

        # Submit a valid delete for all our new objects.
        self._submit_update(self.config_path1,
                            f'{SAMPLE_PERSON}delete: delete\n\n{SAMPLE_KEY_CERT}delete: delete\n\n' +
                            f'{SAMPLE_MNTNER_CLEAN}delete: delete\npassword: crypt-password\n')
        messages = self._retrieve_mails()
        # Expected mails are status, mnt-nfy on mntner (2x), and notify on mntner
        # (notify on PERSON-TEST was removed in the PGP signed update)
        assert len(messages) == 4
        mail_text = self._extract_message_body(messages[0])
        assert messages[0]['Subject'] == 'SUCCESS: my subject'
        assert messages[0]['From'] == 'from@example.com'
        assert messages[0]['To'] == 'Sasha <sasha@example.com>'
        assert '\nDelete succeeded: [person] PERSON-TEST\n' in mail_text
        assert '\nDelete succeeded: [mntner] TEST-MNT\n' in mail_text
        assert '\nDelete succeeded: [key-cert] PGPKEY-80F238C6\n' in mail_text

        self._check_recipients_in_mails(messages[1:], [
            'mnt-nfy@example.net', 'mnt-nfy2@example.net', 'notify@example.net',
        ])

        mnt_nfy_msgs = [msg for msg in messages if msg['To'] in ['mnt-nfy@example.net', 'mnt-nfy2@example.net']]
        self._check_text_in_mails(mnt_nfy_msgs, [
            '\n> Message-ID: <1325754288.4989.6.camel@hostname>\n',
            '\nDelete succeeded for object below: [person] PERSON-TEST:\n',
            '\nDelete succeeded for object below: [mntner] TEST-MNT:\n',
            '\nDelete succeeded for object below: [key-cert] PGPKEY-80F238C6:\n',
            'unįcöde tæst 🌈🦄\n',
            # The object submitted to be deleted has the original name,
            # but when sending delete notifications, they should include the
            # object as currently in the DB, not as submitted in the email.
            'Test person changed by PGP signed update\n',
        ])
        for message in messages[1:]:
            assert message['Subject'] == 'Notification of TEST database changes'
            assert message['From'] == 'from@example.com'

        # Notify attribute mails are only about the objects concerned.
        notify_msg = [msg for msg in messages if msg['To'] == 'notify@example.net'][0]
        mail_text = self._extract_message_body(notify_msg)
        assert notify_msg['Subject'] == 'Notification of TEST database changes'
        assert notify_msg['From'] == 'from@example.com'
        assert '\n> Message-ID: <1325754288.4989.6.camel@hostname>\n' in mail_text
        assert '\nDelete succeeded for object below: [person] PERSON-TEST:\n' not in mail_text
        assert '\nDelete succeeded for object below: [mntner] TEST-MNT:\n' in mail_text
        assert '\nDelete succeeded for object below: [key-cert] PGPKEY-80F238C6:\n' not in mail_text

        # Object should be deleted
        person_text = whois_query('127.0.0.1', self.port_whois1, 'PERSON-TEST')
        assert 'No entries found for the selected source(s)' in person_text
        assert 'PERSON-TEST' not in person_text

        # Object should be deleted from irrd #2 as well through NRTM.
        time.sleep(2)
        person_text = whois_query('127.0.0.1', self.port_whois2, 'PERSON-TEST')
        assert 'No entries found for the selected source(s)' in person_text
        assert 'PERSON-TEST' not in person_text

        # Load the mntner and person again, using the override password
        # Note that the route/route6 objects are RPKI valid on IRRd #1,
        # and RPKI-invalid on IRRd #2
        self._submit_update(self.config_path1,
                            SAMPLE_MNTNER_CLEAN + '\n\n' + SAMPLE_PERSON + '\n\noverride: override-password')
        messages = self._retrieve_mails()
        assert len(messages) == 1
        mail_text = self._extract_message_body(messages[0])
        assert messages[0]['Subject'] == 'SUCCESS: my subject'
        assert messages[0]['From'] == 'from@example.com'
        assert messages[0]['To'] == 'Sasha <sasha@example.com>'
        assert '\nCreate succeeded: [mntner] TEST-MNT\n' in mail_text
        assert '\nCreate succeeded: [person] PERSON-TEST\n' in mail_text
        assert 'email footer' in mail_text
        assert 'Generated by IRRd version ' in mail_text

        # Load samples of all known objects, using the mntner password
        self._submit_update(self.config_path1, LARGE_UPDATE + '\n\npassword: md5-password')
        messages = self._retrieve_mails()
        assert len(messages) == 3
        mail_text = self._extract_message_body(messages[0])
        assert messages[0]['Subject'] == 'SUCCESS: my subject'
        assert messages[0]['From'] == 'from@example.com'
        assert messages[0]['To'] == 'Sasha <sasha@example.com>'
        assert '\nINFO: AS number as065537 was reformatted as AS65537\n' in mail_text
        assert '\nCreate succeeded: [filter-set] FLTR-SETTEST\n' in mail_text
        assert '\nINFO: Address range 192.0.2.0 - 192.0.02.255 was reformatted as 192.0.2.0 - 192.0.2.255\n' in mail_text
        assert '\nINFO: Address prefix 192.0.02.0/24 was reformatted as 192.0.2.0/24\n' in mail_text
        assert '\nINFO: Route set member 2001:0dB8::/48 was reformatted as 2001:db8::/48\n' in mail_text

        # Check whether the objects can be queried from irrd #1,
        # and whether the hash is masked.
        mntner_text = whois_query('127.0.0.1', self.port_whois1, 'TEST-MNT')
        assert 'TEST-MNT' in mntner_text
        assert PASSWORD_HASH_DUMMY_VALUE in mntner_text
        assert 'unįcöde tæst 🌈🦄' in mntner_text
        assert 'PERSON-TEST' in mntner_text

        # (This is the first instance of an object with unicode chars
        # appearing on the NRTM stream.)
        time.sleep(3)
        mntner_text = whois_query('127.0.0.1', self.port_whois2, 'TEST-MNT')
        assert 'TEST-MNT' in mntner_text
        assert PASSWORD_HASH_DUMMY_VALUE in mntner_text
        assert 'unįcöde tæst 🌈🦄' in mntner_text
        assert 'PERSON-TEST' in mntner_text

        # These queries have different responses on #1 than #2,
        # as all IPv4 routes are RPKI invalid on #2.
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!gAS65537')
        assert query_result == '192.0.2.0/24'
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!gAS65547')
        assert query_result == '192.0.2.0/32'  # Pseudo-IRR object from RPKI
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!6AS65537')
        assert query_result == '2001:db8::/48'
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!iRS-TEST')
        assert set(query_result.split(' ')) == {'192.0.2.0/24', '2001:db8::/48'}
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!aAS-SETTEST')
        assert set(query_result.split(' ')) == {'192.0.2.0/24', '2001:db8::/48'}
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!aAS-TESTREF')
        assert set(query_result.split(' ')) == {'192.0.2.0/24', '2001:db8::/48'}
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!a4AS-TESTREF')
        assert query_result == '192.0.2.0/24'
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!a6AS-TESTREF')
        assert query_result == '2001:db8::/48'
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!r192.0.2.0/24')
        assert 'example route' in query_result
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!r192.0.2.0/25,l')
        assert 'example route' in query_result
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!r192.0.2.0/24,L')
        assert 'example route' in query_result
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!r192.0.2.0/23,M')
        assert 'example route' in query_result
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!r192.0.2.0/24,M')
        assert 'RPKI' in query_result  # Does not match the /24, does match the RPKI pseudo-IRR /32
        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!r192.0.2.0/24,o')
        assert query_result == 'AS65537'
        query_result = whois_query('127.0.0.1', self.port_whois1, '-x 192.0.02.0/24')
        assert 'example route' in query_result
        query_result = whois_query('127.0.0.1', self.port_whois1, '-l 192.0.02.0/25')
        assert 'example route' in query_result
        query_result = whois_query('127.0.0.1', self.port_whois1, '-L 192.0.02.0/24')
        assert 'example route' in query_result
        query_result = whois_query('127.0.0.1', self.port_whois1, '-M 192.0.02.0/23')
        assert 'example route' in query_result
        query_result = whois_query('127.0.0.1', self.port_whois1, '-i member-of RS-test')
        assert 'example route' in query_result

        query_result = whois_query_irrd('127.0.0.1', self.port_whois2, '!gAS65537')
        assert not query_result
        query_result = whois_query_irrd('127.0.0.1', self.port_whois2, '!6AS65537')
        assert query_result == '2001:db8::/48'
        query_result = whois_query_irrd('127.0.0.1', self.port_whois2, '!iRS-TEST')
        assert query_result == '2001:db8::/48'
        query_result = whois_query_irrd('127.0.0.1', self.port_whois2, '!aAS-SETTEST')
        assert query_result == '2001:db8::/48'
        query_result = whois_query_irrd('127.0.0.1', self.port_whois2, '!aAS-TESTREF')
        assert query_result == '2001:db8::/48'
        query_result = whois_query('127.0.0.1', self.port_whois2, '-x 192.0.02.0/24')
        assert 'example route' not in query_result
        query_result = whois_query_irrd('127.0.0.1', self.port_whois2, '!r192.0.2.0/24,L')
        assert 'RPKI' in query_result  # Pseudo-IRR object 0/0 from RPKI

        # These queries should produce identical answers on both instances.
        for port in self.port_whois1, self.port_whois2:
            query_result = whois_query_irrd('127.0.0.1', port, '!iAS-SETTEST')
            assert set(query_result.split(' ')) == {'AS65537', 'AS65538', 'AS65539'}
            query_result = whois_query_irrd('127.0.0.1', port, '!iAS-TESTREF')
            assert set(query_result.split(' ')) == {'AS-SETTEST', 'AS65540'}
            query_result = whois_query_irrd('127.0.0.1', port, '!iAS-TESTREF,1')
            assert set(query_result.split(' ')) == {'AS65537', 'AS65538', 'AS65539', 'AS65540'}
            query_result = whois_query_irrd('127.0.0.1', port, '!maut-num,as65537')
            assert 'AS65537' in query_result
            assert 'TEST-AS' in query_result
            query_result = whois_query_irrd('127.0.0.1', port, '!oTEST-MNT')
            assert 'AS65537' in query_result
            assert 'TEST-AS' in query_result
            assert 'AS65536 - AS65538' in query_result
            assert 'rtrs-settest' in query_result
            query_result = whois_query('127.0.0.1', port, '-T route6 -i member-of RS-TEST')
            assert 'No entries found for the selected source(s)' in query_result
            query_result = whois_query('127.0.0.1', port, 'dashcare')
            assert 'ROLE-TEST' in query_result

        query_result = whois_query_irrd('127.0.0.1', self.port_whois1, '!j-*')
        assert query_result == 'TEST:Y:1-29:29\nRPKI:N:-'
        # irrd #2 missed the first update from NRTM, as they were done at
        # the same time and loaded from the full export, so its serial should
        # start at 2 rather than 1.
        query_result = whois_query_irrd('127.0.0.1', self.port_whois2, '!j-*')
        assert query_result == 'TEST:Y:2-29:29\nRPKI:N:-'

    def _start_mailserver(self):
        """
        Start the mailserver through twisted. This special SMTP server is
        configured as the SMTP server for both IRRd instances.
        It keeps mails in memory, and _retrieve_mails() can retrieve them
        using special SMTP commands.
        """
        self.pidfile_mailserver = str(self.tmpdir) + '/mailserver.pid'
        self.logfile_mailserver = str(self.tmpdir) + '/mailserver.log'
        mailserver_path = IRRD_ROOT_PATH + '/irrd/integration_tests/mailserver.tac'
        assert not subprocess.call(['twistd', f'--pidfile={self.pidfile_mailserver}',
                                    f'--logfile={self.logfile_mailserver}', '-y', mailserver_path])

    # noinspection PyTypeChecker
    def _start_irrds(self):
        """
        Configure and start two independent instances of IRRd.
        IRRd #1 has an authoritative database, IRRd #2 mirrors that database
        from #1.
        """
        self.database_url1 = os.environ['IRRD_DATABASE_URL_INTEGRATION_1']
        self.database_url2 = os.environ['IRRD_DATABASE_URL_INTEGRATION_2']

        self.config_path1 = str(self.tmpdir) + '/irrd1_config.yaml'
        self.config_path2 = str(self.tmpdir) + '/irrd2_config.yaml'
        self.logfile1 = str(self.tmpdir) + '/irrd1.log'
        self.logfile2 = str(self.tmpdir) + '/irrd2.log'
        self.pidfile1 = str(self.tmpdir) + '/irrd1.pid'
        self.pidfile2 = str(self.tmpdir) + '/irrd2.pid'
        self.roa_source1 = str(self.tmpdir) + '/roa1.json'
        self.roa_source2 = str(self.tmpdir) + '/roa2.json'
        self.export_dir1 = str(self.tmpdir) + '/export1/'
        self.export_dir2 = str(self.tmpdir) + '/export2/'
        os.mkdir(self.export_dir1)
        os.mkdir(self.export_dir2)

        print(textwrap.dedent(f"""
            Preparing to start IRRd for integration test.
            
            IRRd #1 running on HTTP port {self.port_http1}, whois port {self.port_whois1}
            Config in: {self.config_path1}
            Database URL: {self.database_url1}
            PID file: {self.pidfile1}
            Logfile: {self.logfile1}

            IRRd #2 running on HTTP port {self.port_http2}, whois port {self.port_whois2}
            Config in: {self.config_path2}
            Database URL: {self.database_url2}
            PID file: {self.pidfile2}
            Logfile: {self.logfile2}
        """))

        with open(self.roa_source1, 'w') as roa_file:
            ujson.dump({'roas': [{'prefix': '192.0.2.0/32', 'asn': 'AS65547', 'maxLength': '32', 'ta': 'TA'}]}, roa_file)
        with open(self.roa_source2, 'w') as roa_file:
            ujson.dump({'roas': [{'prefix': '0/0', 'asn': 'AS0', 'maxLength': '0', 'ta': 'TA'}]}, roa_file)

        base_config = {
            'irrd': {
                'access_lists': {
                    'localhost': ['::/32', '127.0.0.1']
                },

                'server': {
                    'http': {
                        'access_list': 'localhost',
                        'interface': '::0',
                        'port': 8080
                    },
                    'whois': {
                        'interface': '::0',
                        'max_connections': 50,
                        'port': 8043
                    },
                },

                'auth': {
                    'gnupg_keyring': None,
                    'override_password': '$1$J6KycItM$MbPaBU6iFSGFV299Rk7Di0',
                },

                'email': {
                    'footer': 'email footer',
                    'from': 'from@example.com',
                    'smtp': f'localhost:{EMAIL_SMTP_PORT}',
                },

                'log': {
                    'logfile_path': None,
                    'level': 'DEBUG',
                },

                'sources': {}
            }
        }

        config1 = base_config.copy()
        config1['irrd']['database_url'] = self.database_url1
        config1['irrd']['server']['http']['port'] = self.port_http1
        config1['irrd']['server']['whois']['port'] = self.port_whois1
        config1['irrd']['auth']['gnupg_keyring'] = str(self.tmpdir) + '/gnupg1'
        config1['irrd']['log']['logfile_path'] = self.logfile1
        config1['irrd']['rpki'] = {'roa_source': 'file://' + self.roa_source1}
        config1['irrd']['sources']['TEST'] = {
            'authoritative': True,
            'keep_journal': True,
            'export_destination': self.export_dir1,
            'export_timer': '1',
            'nrtm_access_list': 'localhost',
        }
        with open(self.config_path1, 'w') as yaml_file:
            yaml.safe_dump(config1, yaml_file)

        config2 = base_config.copy()
        config2['irrd']['database_url'] = self.database_url2
        config2['irrd']['server']['http']['port'] = self.port_http2
        config2['irrd']['server']['whois']['port'] = self.port_whois2
        config2['irrd']['auth']['gnupg_keyring'] = str(self.tmpdir) + '/gnupg2'
        config2['irrd']['log']['logfile_path'] = self.logfile2
        config2['irrd']['rpki'] = {'roa_source': 'file://' + self.roa_source2}
        config2['irrd']['sources']['TEST'] = {
            'keep_journal': True,
            'import_serial_source': f'file://{self.export_dir1}/TEST.CURRENTSERIAL',
            'import_source': f'file://{self.export_dir1}/test.db.gz',
            'export_destination': self.export_dir2,
            'import_timer': '1',
            'export_timer': '1',
            'nrtm_host': '127.0.0.1',
            'nrtm_port': str(self.port_whois1),
            'nrtm_access_list': 'localhost',
        }
        with open(self.config_path2, 'w') as yaml_file:
            yaml.safe_dump(config2, yaml_file)

        self._prepare_database()

        assert not subprocess.call(['twistd', f'--pidfile={self.pidfile1}', 'irrd', f'--config={self.config_path1}'])
        assert not subprocess.call(['twistd', f'--pidfile={self.pidfile2}', 'irrd', f'--config={self.config_path2}'])

    def _prepare_database(self):
        """
        Prepare the databases for IRRd #1 and #2. This includes running
        migrations to create tables, and *wiping existing content*.
        """
        config_init(self.config_path1)
        alembic_cfg = config.Config()
        alembic_cfg.set_main_option('script_location', f'{IRRD_ROOT_PATH}/irrd/storage/alembic')
        command.upgrade(alembic_cfg, 'head')

        connection = sa.create_engine(self.database_url1).connect()
        connection.execute('DELETE FROM rpsl_objects')
        connection.execute('DELETE FROM rpsl_database_journal')
        connection.execute('DELETE FROM database_status')

        config_init(self.config_path2)
        alembic_cfg = config.Config()
        alembic_cfg.set_main_option('script_location', f'{IRRD_ROOT_PATH}/irrd/storage/alembic')
        command.upgrade(alembic_cfg, 'head')

        connection = sa.create_engine(self.database_url2).connect()
        connection.execute('DELETE FROM rpsl_objects')
        connection.execute('DELETE FROM rpsl_database_journal')
        connection.execute('DELETE FROM database_status')

    def _submit_update(self, config_path, request):
        """
        Submit an update to an IRRd by calling the email submission process
        with a specific config path. Request is the raw RPSL update, possibly
        signed with inline PGP.
        """
        email = textwrap.dedent("""
            From submitter@example.com@localhost  Thu Jan  5 10:04:48 2018
            Received: from [127.0.0.1] (localhost.localdomain [127.0.0.1])
              by hostname (Postfix) with ESMTPS id 740AD310597
              for <irrd@example.com>; Thu,  5 Jan 2018 10:04:48 +0100 (CET)
            Message-ID: <1325754288.4989.6.camel@hostname>
            Subject: my subject
            Subject: not my subject
            From: Sasha <sasha@example.com>
            To: sasha@localhost
            Date: Thu, 05 Jan 2018 10:04:48 +0100
            X-Mailer: Python 3.7
            Content-Transfer-Encoding: base64
            Content-Type: text/plain; charset=utf-8
            Mime-Version: 1.0

        """).lstrip().encode('utf-8')
        email += base64.b64encode(request.encode('utf-8'))

        script = IRRD_ROOT_PATH + '/irrd/scripts/submit_email.py'
        p = subprocess.Popen([script, f'--config={config_path}', f'--irrd_pidfile={self.pidfile1}'],
                             stdin=subprocess.PIPE)
        p.communicate(email)
        p.wait()

    def _retrieve_mails(self):
        """
        Retrieve all mails kept in storage by the special integration test
        SMTP server. Returns a list of email.Message objects.
        Will only return new mails since the last call.
        """
        s = socket.socket()
        s.settimeout(5)
        s.connect(('localhost', EMAIL_SMTP_PORT))

        s.sendall(f'{EMAIL_RETURN_MSGS_COMMAND}\r\n'.encode('ascii'))

        buffer = b''
        while EMAIL_END not in buffer:
            data = s.recv(1024 * 1024)
            buffer += data
        buffer = buffer.split(b'\n', 1)[1]
        buffer = buffer.split(EMAIL_END, 1)[0]

        s.sendall(f'{EMAIL_DISCARD_MSGS_COMMAND}\r\n'.encode('ascii'))
        messages = [email.message_from_string(m.strip().decode('ascii')) for m in buffer.split(EMAIL_SEPARATOR.encode('ascii'))]
        return messages

    def _extract_message_body(self, message):
        """
        Convenience method to extract the main body from a non-multipart
        email.Message object.
        """
        charset = message.get_content_charset(failobj='ascii')
        return message.get_payload(decode=True).decode(charset, 'backslashreplace')  # type: ignore

    def _check_text_in_mails(self, messages, expected_texts):
        """
        Check a list of email.Message objects for each of a list of
        expected texts. I.e. every message should contain every text.
        """
        for expected_text in expected_texts:
            for message in messages:
                message_text = self._extract_message_body(message)
                assert expected_text in message_text, f'Missing text {expected_text} in mail:\n{message_text}'

    def _check_recipients_in_mails(self, messages, expected_recipients):
        """
        Check whether a list of email.Message objects match a list of
        expected email recipients, in any order.

        Order may very due to unordered data structures being used when
        generating some notifications.
        """
        assert len(messages) == len(expected_recipients)
        original_expected_recipients = set(expected_recipients)
        leftover_expected_recipients = original_expected_recipients.copy()
        for message in messages:
            for recipient in original_expected_recipients:
                if message['To'] == recipient:
                    leftover_expected_recipients.remove(recipient)
        assert not leftover_expected_recipients

    def teardown_method(self, method):
        """
        This teardown method is always called after tests complete, whether
        or not they succeed. It is used to kill any leftover IRRd or SMTP
        server processes.
        """
        for pidfile in self.pidfile1, self.pidfile2, self.pidfile_mailserver:
            try:
                with open(pidfile) as fh:
                    os.kill(int(fh.read()), signal.SIGTERM)
            except (FileNotFoundError, ProcessLookupError, ValueError):
                pass


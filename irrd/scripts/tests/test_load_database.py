from unittest.mock import Mock

from irrd.utils.test_utils import flatten_mock_calls
from ..load_database import load


def test_load_database_success(capsys, monkeypatch):
    mock_dh = Mock()
    monkeypatch.setattr('irrd.scripts.load_database.DatabaseHandler', lambda: mock_dh)
    mock_parser = Mock()
    monkeypatch.setattr('irrd.scripts.load_database.MirrorFileImportParser', lambda *args, **kwargs: mock_parser)
    mock_parser.run_import = lambda: None

    assert load('TEST', 'test.db', 42) == 0
    assert flatten_mock_calls(mock_dh) == [
        ['delete_all_rpsl_objects_with_journal', ('TEST',), {}],
        ['disable_journaling', (), {}],
        ['commit', (), {}],
        ['close', (), {}]
    ]

    # run_import() call is not included here
    assert flatten_mock_calls(mock_parser) == []
    assert not capsys.readouterr().out


def test_load_database_import_error(capsys, monkeypatch, caplog):
    mock_dh = Mock()
    monkeypatch.setattr('irrd.scripts.load_database.DatabaseHandler', lambda: mock_dh)
    mock_parser = Mock()
    monkeypatch.setattr('irrd.scripts.load_database.MirrorFileImportParser', lambda *args, **kwargs: mock_parser)
    mock_parser.run_import = lambda: 'object-parsing-error'

    assert load('TEST', 'test.db', 42) == 1
    assert flatten_mock_calls(mock_dh) == [
        ['delete_all_rpsl_objects_with_journal', ('TEST',), {}],
        ['disable_journaling', (), {}],
        ['rollback', (), {}],
        ['close', (), {}]
    ]

    # run_import() call is not included here
    assert flatten_mock_calls(mock_parser) == []

    assert 'object-parsing-error' not in caplog.text
    stdout = capsys.readouterr().out
    assert 'Error occurred while processing object:\nobject-parsing-error' in stdout

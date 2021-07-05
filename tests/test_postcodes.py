from ibsg import postcodes


def test_main(datadir, monkeypatch):
    def _mock_cwd(*args, **kwargs):
        return datadir

    def _mock_request_public_ber_db(*args, **kwargs):
        return None

    monkeypatch.setattr("ibsg.Path.cwd", _mock_cwd)
    monkeypatch.setattr(
        "ibsg.postcodes.request_public_ber_db", _mock_request_public_ber_db
    )
    postcodes.main(email_address="i_am_a_test")

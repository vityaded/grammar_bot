from bot.handlers import _should_attach_remediation


def test_should_attach_remediation():
    assert _should_attach_remediation("almost", "normal", False) is False
    assert _should_attach_remediation("wrong", "normal", False) is True
    assert _should_attach_remediation("almost", "strict", False) is True
    assert _should_attach_remediation("wrong", "strict", True) is False

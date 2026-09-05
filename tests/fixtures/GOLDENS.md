# Golden fixture maintenance

The core command and response JSON files are fixed regression expectations.
Do not regenerate their expected bytes or decoded values from the implementation
being tested. That can turn a regression into the new expected behavior.

For a command change, compare the generated request with an independently
retained capture or documented packet layout. For a parser change, review the
expected fields against the input bytes and their recorded evidence. Change
only the affected cases and explain the evidence in the change description.
Record any capture extraction or normalization and its source digest.

These files historically could be regenerated from NetAudio. Their existing
expected outputs therefore provide regression coverage, not independent proof
of protocol semantics. The regeneration script and Make target have been
removed; all its additional cases were already present in the fixture files.
Existing source fixtures and provenance records remain unchanged.

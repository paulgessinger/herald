from herald.github import GitHub


def test_file_key_without_png():
    assert (
        GitHub._get_file_key("owner/repo", 42, "dir/file.txt", False)
        == "file_owner/repo_42_dir/file.txt"
    )


def test_file_key_with_png():
    assert (
        GitHub._get_file_key("owner/repo", 42, "dir/file.pdf", True)
        == "file_owner/repo_42_dir/file.pdf_png"
    )


def test_file_key_changes_when_png_toggled():
    a = GitHub._get_file_key("owner/repo", 42, "f.pdf", False)
    b = GitHub._get_file_key("owner/repo", 42, "f.pdf", True)
    assert a != b


def test_artifact_key():
    assert GitHub._get_artifact_key("owner/repo", 42) == "artifact_owner/repo_42"


def test_artifact_key_includes_repo():
    a = GitHub._get_artifact_key("alpha/x", 42)
    b = GitHub._get_artifact_key("beta/x", 42)
    assert a != b


def test_zip_path_strips_surrounding_slashes():
    assert GitHub._zip_path("/foo/bar/") == "foo/bar"
    assert GitHub._zip_path("foo/bar") == "foo/bar"
    assert GitHub._zip_path("/foo") == "foo"
    assert GitHub._zip_path("") == ""

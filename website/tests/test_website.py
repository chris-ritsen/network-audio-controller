import os
import shutil
from pathlib import Path

import pytest

from website.publish import activate_release, publish_source
from website.validate import PUBLIC_DIRECTORY, validate_website


def test_static_website_contract() -> None:
    assert validate_website() == []


@pytest.mark.skipif(os.name != "posix", reason="publication requires POSIX filesystem operations")
def test_atomic_publication_and_rollback(tmp_path: Path) -> None:
    source_directory = tmp_path / "source"
    deployment_root = tmp_path / "deployment"
    shutil.copytree(PUBLIC_DIRECTORY, source_directory)
    deployment_root.mkdir()

    first_identifier, first_previous, first_target = publish_source(
        source_directory,
        deployment_root,
        os.getuid(),
        os.getgid(),
    )
    assert first_previous is None
    assert first_target == f"releases/{first_identifier}/public"
    assert (deployment_root / "current").resolve() == deployment_root / first_target

    index_path = source_directory / "index.html"
    index_path.write_text(index_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    second_identifier, second_previous, second_target = publish_source(
        source_directory,
        deployment_root,
        os.getuid(),
        os.getgid(),
    )
    assert second_identifier != first_identifier
    assert second_previous == first_target
    assert second_target == f"releases/{second_identifier}/public"

    rollback_previous, rollback_target = activate_release(deployment_root, first_identifier)
    assert rollback_previous == second_target
    assert rollback_target == first_target
    assert (deployment_root / "current").resolve() == deployment_root / first_target


@pytest.mark.skipif(os.name != "posix", reason="publication requires POSIX filesystem operations")
def test_publication_rejects_source_symlinks(tmp_path: Path) -> None:
    source_directory = tmp_path / "source"
    deployment_root = tmp_path / "deployment"
    shutil.copytree(PUBLIC_DIRECTORY, source_directory)
    deployment_root.mkdir()
    (source_directory / "assets" / "forbidden").symlink_to(source_directory / "index.html")

    with pytest.raises(ValueError, match="forbidden path"):
        publish_source(
            source_directory,
            deployment_root,
            os.getuid(),
            os.getgid(),
        )

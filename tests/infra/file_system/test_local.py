from pathlib import Path

from modules.infra.file_system.local import LocalFS


def test_local_fs_resolves_paths_against_base_path(tmp_path: Path) -> None:
    handler = LocalFS(base_path=tmp_path / "dag_verification")

    handler.write(file_path="check_file.txt", content="dag_verification")

    expected_file = tmp_path / "dag_verification" / "check_file.txt"
    assert expected_file.exists()
    assert handler.exists(file_path="check_file.txt")
    assert handler.get_absolute_path("check_file.txt") == expected_file
    assert handler.get_metadata(file_path="check_file.txt").name == "check_file.txt"

    handler.delete(file_path="check_file.txt")
    assert not handler.exists(file_path="check_file.txt")

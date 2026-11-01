import datetime as dt
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "get-next-version.sh"


class GetNextVersionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.remote = self.root / "remote"
        self.checkout = self.root / "checkout"
        self.year = dt.date.today().year
        self.git(self.root, "-c", "init.templateDir=", "init", "-q", "-b", "main", str(self.remote))
        self.git(self.remote, "config", "user.name", "Test")
        self.git(self.remote, "config", "user.email", "test@example.invalid")
        self.git(self.remote, "config", "commit.gpgsign", "false")
        self.git(self.remote, "config", "tag.gpgsign", "false")
        for message in ("Initial commit", "Latest commit"):
            self.git(self.remote, "commit", "-q", "--allow-empty", "-m", message)
        self.git(
            self.root, "clone", "-q", "--depth=1", "--no-tags",
            self.remote.as_uri(), str(self.checkout),
        )

    def git(self, cwd, *args):
        return subprocess.run(
            ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
        ).stdout.strip()

    def run_version(self):
        return subprocess.run(
            ["bash", str(SCRIPT)], cwd=self.checkout, capture_output=True, text=True
        )

    def assert_version(self, number):
        result = self.run_version()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, f"{self.year}.{number}\n")

    def test_no_tags_starts_at_one(self):
        self.assert_version(1)

    def test_previous_year_tags_start_a_new_sequence(self):
        self.git(self.remote, "tag", f"{self.year - 1}.999")

        self.assert_version(1)

    def test_numeric_order_and_only_current_year_release_tags(self):
        for tag in (
            f"{self.year}.9", f"{self.year}.2", f"{self.year}.10",
            f"{self.year - 1}.999", f"{self.year}.99-rc1", f"{self.year}.100.extra",
        ):
            self.git(self.remote, "tag", tag)

        self.assertEqual(self.git(self.checkout, "rev-parse", "--is-shallow-repository"), "true")
        self.assertEqual(self.git(self.checkout, "tag", "-l"), "")
        self.assert_version(11)
        self.assertEqual(self.git(self.checkout, "tag", "-l"), "")

    def test_annotated_tags_are_supported(self):
        self.git(self.remote, "tag", "-a", f"{self.year}.4", "-m", "Release")

        self.assert_version(5)

    def test_leading_zeroes_are_decimal(self):
        self.git(self.remote, "tag", f"{self.year}.009")

        self.assert_version(10)

    def test_remote_failure_does_not_emit_a_version(self):
        self.git(self.checkout, "remote", "set-url", "origin", str(self.root / "missing"))

        result = self.run_version()

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()

from aperturedb.DescriptorDataCSV import DescriptorDataCSV
from aperturedb.ParallelLoader import ParallelLoader
from io import BytesIO, TextIOWrapper
import sys

# stats had some issues with displaying of computed data.
# These tests reproduce the issue.
# Run it with: (to avoid pytest's output)
# pytest  test_Stats.py -s --no-summary


class TestStats():
    def ingest_with_capture(self, data, db):
        loader = ParallelLoader(db)
        # setup the environment
        old_stdout = sys.stdout
        sys.stdout = TextIOWrapper(BytesIO(), sys.stdout.encoding)

        loader.ingest(data, batchsize=99, numthreads=31, stats=True)
        sys.stdout.seek(0)      # jump to the start
        out = sys.stdout.read()  # read output

        sys.stdout = old_stdout
        return out

    def validate_stats(self, out, assertions):
        seen = set()
        for line in out.splitlines():
            if ":" in line:
                stats = line.split(":")
                if len(stats) == 2:
                    first, second = line.split(":")
                    first_stripped = first.strip()
                    print(first, second)
                    if first_stripped in assertions:
                        assert assertions[first_stripped](second.strip()) is True, (
                            f"Assertion failed for '{first}' "
                            f"with value {second}"
                        )
                        seen.add(first_stripped)
        
        missing = set(assertions.keys()) - seen
        assert not missing, f"Missing stats output for keys: {missing}"

    def test_stats_all_errors_non_equal_last_batch(self, db, utils):
        utils.remove_all_objects()
        # Try to ingest descriptors, with no descriptor set, so all queries fail
        data = DescriptorDataCSV(
            "./input/setA.adb.csv", blobs_relative_to_csv=True)
        out = self.ingest_with_capture(data, db)
        assertions = {
            "Total inserted elements": lambda x: float(x) == 0,
            "Overall insertion throughput (element/s)": lambda x: float(x) == 0,
        }
        self.validate_stats(out, assertions)

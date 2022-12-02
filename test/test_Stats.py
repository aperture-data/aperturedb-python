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
        for line in out.splitlines():
            if ":" in line:
                stats = line.split(":")
                if len(stats) == 2:
                    first, second = line.split(":")
                    print(first, second)
                    if first in assertions:
                        assert assertions[first.strip()](second.strip()) == True,\
                            f"Assertion failed for '{first}' with value {second}"

    def test_stats_all_errors_non_equal_last_batch(self, db, utils):
        utils.remove_all_objects()
        # Try to ingest descriptors, with no descriptor set, so all queries fail
        data = DescriptorDataCSV("./input/setA.adb.csv")
        out = self.ingest_with_capture(data, db)
        assertions = {
            "Total inserted elements": lambda x: float(x) == 0,
            "Overall insertion throughput (element/s)": lambda x: x == "NaN",
        }
        self.validate_stats(out, assertions)

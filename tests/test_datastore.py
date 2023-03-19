# unittests for datastore.py

import unittest
import time
import os
import shutil
from storage.file import File

def testdata():
    data = []
    for i in range(1):
        metrics = f"test{i}"
        for idx in range(100000):
            if 10000 < idx < 100000:
                cat = "cat1"
            elif 100 < idx < 10000:
                cat = "cat2"
            else:
                cat = "cat3"

            if 90000 < idx < 100000:
                type = "type1"
            elif 1000 < idx < 90000:
                type = "type2"
            else:
                type = "type3"

            data.append(
                {
                    "timestamp": time.time(),
                    "metrics": metrics,
                    "value": idx,
                    "tags": {"Category": cat, "Type": type}
                }
            )
    return data

class DataStoreTest(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self._storage = File("testdata")

    def tearDown(self) -> None:
        shutil.rmtree("testdata")

    def test_file_storage(self):
        data = testdata()
        ret = self._storage.write(data)
        self.assertTrue(ret)

        tags = {'Category': 'cat3', 'Type': 'type2'}
        result = self._storage.read("test0", tags=tags)
        self.assertEqual(len(result), 1)
        print(result)

        tags = {'Category': 'cat3', 'Type': 'type3'}
        result = self._storage.read("test0", tags=tags)
        self.assertTrue(len(result) > 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
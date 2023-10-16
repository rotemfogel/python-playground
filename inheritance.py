import asyncio
from abc import ABC
from typing import Any
from unittest import IsolatedAsyncioTestCase, main


class BaseTest(IsolatedAsyncioTestCase, ABC):
    def __init__(self, multi_mode: bool, **kwargs: Any):
        super().__init__(**kwargs)
        self.multi_mode = multi_mode

    async def test_execute(self):
        print(f"multi_mode: {self.multi_mode}")
        self.assertTrue(True)

    def runTest(self):
        asyncio.run(self.test_execute())


class TestTrue(BaseTest):
    def __init__(self, *args, **kwargs: Any):
        super(TestTrue, self).__init__(multi_mode=True, **kwargs)


class TestFalse(BaseTest):
    def __init__(self, *args, **kwargs: Any):
        super(TestFalse, self).__init__(multi_mode=False, **kwargs)


if __name__ == "__main__":
    main()

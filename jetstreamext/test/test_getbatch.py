# Copyright 2025 Oliver Lambson
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from nats import NATS
from nats.aio.msg import Msg
from nats.js import JetStreamContext, api

import jetstreamext


@pytest.mark.asyncio
async def test_get_batch(js_client: JetStreamContext):
    """Test get_batch with various option combinations (port of Go TestGetBatch)"""
    js = js_client

    # Create stream
    await js.add_stream(
        api.StreamConfig(
            name="TEST",
            subjects=["foo.*"],
            allow_direct=True,
        )
    )

    # Publish some messages
    for _ in range(5):
        await js.publish("foo.A", b"msg")
        await js.publish("foo.B", b"msg")

    await asyncio.sleep(0.1)  # 100ms pause
    pause = datetime.now(timezone.utc)

    for _ in range(5):
        await js.publish("foo.A", b"msg")
        await js.publish("foo.B", b"msg")

    test_cases = [
        {
            "name": "no options provided, get 10 messages",
            "batch": 10,
            "opts": [],
            "expected_msgs": 10,
            "expected_seqs": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        },
        {
            "name": "get 5 messages from sequence 5",
            "batch": 5,
            "opts": [jetstreamext.get_batch_seq(5)],
            "expected_msgs": 5,
            "expected_seqs": [5, 6, 7, 8, 9],
        },
        {
            "name": "get 5 messages from subject foo.B",
            "batch": 5,
            "opts": [jetstreamext.get_batch_subject("foo.B")],
            "expected_msgs": 5,
            "expected_seqs": [2, 4, 6, 8, 10],
        },
        {
            "name": "get 5 messages from sequence 5 and subject foo.B",
            "batch": 5,
            "opts": [
                jetstreamext.get_batch_seq(5),
                jetstreamext.get_batch_subject("foo.B"),
            ],
            "expected_msgs": 5,
            "expected_seqs": [6, 8, 10, 12, 14],
        },
        {
            "name": "get more messages than available",
            "batch": 10,
            "opts": [jetstreamext.get_batch_seq(16)],
            "expected_msgs": 5,
            "expected_seqs": [16, 17, 18, 19, 20],
        },
        {
            "name": "with max bytes",
            "batch": 10,
            "opts": [jetstreamext.get_batch_max_bytes(15)],
            "expected_msgs": 2,
            "expected_seqs": [1, 2],
        },
        {
            "name": "seq higher than available",
            "batch": 10,
            "opts": [jetstreamext.get_batch_seq(21)],
            "expected_msgs": 0,
            "expected_seqs": [],
            "expect_no_messages": True,
        },
        {
            "name": "with start time",
            "batch": 10,
            "opts": [jetstreamext.get_batch_start_time(pause)],
            "expected_msgs": 10,
            "expected_seqs": [11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
        },
    ]

    for test_case in test_cases:
        print(f"Running test: {test_case['name']}")

        try:
            messages = [
                msg
                async for msg in jetstreamext.get_batch(
                    js, "TEST", test_case["batch"], *test_case["opts"]
                )
            ]
        except jetstreamext.NoMessagesError:
            messages = []
            if test_case.get("expect_no_messages"):
                continue
            else:
                raise

        assert len(messages) == test_case["expected_msgs"], (
            f"Test '{test_case['name']}': Expected {test_case['expected_msgs']} messages, got {len(messages)}"
        )

        for i, msg in enumerate(messages):
            expected_seq = test_case["expected_seqs"][i]
            assert msg.seq == expected_seq, (
                f"Test '{test_case['name']}': Expected sequence {expected_seq}, got {msg.seq}"
            )


@pytest.mark.asyncio
async def test_get_batch_invalid_options():
    """Test get_batch with invalid options"""

    # Test invalid start time and sequence exclusive
    with pytest.raises(
        jetstreamext.InvalidOptionError,
        match="cannot set both start time and sequence number",
    ):
        pause = datetime.now(timezone.utc)
        opts = [jetstreamext.get_batch_start_time(pause), jetstreamext.get_batch_seq(5)]
        # The error is raised when creating the options
        opt_obj = jetstreamext.getbatch._GetBatchOpts(batch=10)
        for opt in opts:
            opt(opt_obj)

    # Test invalid max bytes
    with pytest.raises(
        jetstreamext.InvalidOptionError, match="max bytes has to be greater than 0"
    ):
        jetstreamext.get_batch_max_bytes(0)

    # Test invalid sequence
    with pytest.raises(
        jetstreamext.InvalidOptionError,
        match="sequence number has to be greater than 0",
    ):
        jetstreamext.get_batch_seq(0)


@pytest.mark.asyncio
async def test_get_last_msgs_for(js_client: JetStreamContext):
    """Test get_last_msgs_for with various option combinations (port of Go TestGetLastMessagesFor)"""
    js = js_client

    # Create stream
    await js.add_stream(
        api.StreamConfig(
            name="TEST",
            subjects=["foo.*"],
            allow_direct=True,
        )
    )

    # Publish messages in specific order (matching Go test exactly):
    # foo.A (1), foo.A (2), foo.B (3), foo.A (4), foo.B (5)
    await js.publish("foo.A", b"msg")
    await js.publish("foo.A", b"msg")
    await js.publish("foo.B", b"msg")
    await js.publish("foo.A", b"msg")
    await js.publish("foo.B", b"msg")

    # Pause here to test up_to_time
    pause = datetime.now(timezone.utc)
    await asyncio.sleep(0.1)  # 100ms delay

    await js.publish("foo.B", b"msg")  # seq 6
    await js.publish("foo.C", b"msg")  # seq 7

    test_cases = [
        {
            "name": "match all subjects",
            "subjects": ["foo.*"],
            "opts": [],
            "expected_msgs": 3,
            "expected_seqs": [4, 6, 7],
        },
        {
            "name": "match single subject",
            "subjects": ["foo.A"],
            "opts": [],
            "expected_msgs": 1,
            "expected_seqs": [4],
        },
        {
            "name": "match multiple subjects",
            "subjects": ["foo.A", "foo.B"],
            "opts": [],
            "expected_msgs": 2,
            "expected_seqs": [4, 6],
        },
        {
            "name": "match all up to sequence",
            "subjects": ["foo.*"],
            "opts": [jetstreamext.get_last_msgs_up_to_seq(3)],
            "expected_msgs": 2,
            "expected_seqs": [2, 3],
        },
        {
            "name": "match all up to time",
            "subjects": ["foo.*"],
            "opts": [jetstreamext.get_last_msgs_up_to_time(pause)],
            "expected_msgs": 2,
            "expected_seqs": [4, 5],
        },
        {
            "name": "with batch size",
            "subjects": ["foo.*"],
            "opts": [jetstreamext.get_last_msgs_batch_size(2)],
            "expected_msgs": 2,
            "expected_seqs": [4, 6],
        },
        {
            "name": "no messages match filter",
            "subjects": ["foo.Z"],
            "opts": [],
            "expected_msgs": 0,
            "expected_seqs": [],
            "expect_no_messages": True,
        },
    ]

    for test_case in test_cases:
        print(f"Running test: {test_case['name']}")

        try:
            messages = [
                msg
                async for msg in jetstreamext.get_last_msgs_for(
                    js, "TEST", test_case["subjects"], *test_case["opts"]
                )
            ]
        except jetstreamext.NoMessagesError:
            messages = []
            if test_case.get("expect_no_messages"):
                continue
            else:
                raise

        assert len(messages) == test_case["expected_msgs"], (
            f"Test '{test_case['name']}': Expected {test_case['expected_msgs']} messages, got {len(messages)}"
        )

        for i, msg in enumerate(messages):
            expected_seq = test_case["expected_seqs"][i]
            assert msg.seq == expected_seq, (
                f"Test '{test_case['name']}': Expected sequence {expected_seq}, got {msg.seq}"
            )


@pytest.mark.asyncio
async def test_get_last_msgs_for_invalid_options():
    """Test get_last_msgs_for with invalid options"""

    # Test empty subjects
    with pytest.raises(
        jetstreamext.SubjectRequiredError, match="at least one subject is required"
    ):
        # Create a mock client to pass proper types
        mock_js = MagicMock(spec=JetStreamContext)
        async for _ in jetstreamext.get_last_msgs_for(mock_js, "TEST", []):
            pass

    # Test time and sequence exclusive
    with pytest.raises(
        jetstreamext.InvalidOptionError,
        match="cannot set both up to sequence and up to time",
    ):
        pause = datetime.now(timezone.utc)
        opts = [
            jetstreamext.get_last_msgs_up_to_time(pause),
            jetstreamext.get_last_msgs_up_to_seq(3),
        ]
        # The error is raised when creating the options
        opt_obj = jetstreamext.getbatch._GetLastBatchOpts(multi_last_for=["foo.*"])
        for opt in opts:
            opt(opt_obj)

    # Test invalid batch size
    with pytest.raises(
        jetstreamext.InvalidOptionError, match="batch size has to be greater than 0"
    ):
        jetstreamext.get_last_msgs_batch_size(0)


@pytest.mark.asyncio
async def test_convert_direct_get_msg_response_to_msg():
    """Test convertDirectGetMsgResponseToMsg function (port of Go TestConvertDirectGetMsgResponseToMsg)"""

    # Create a mock NATS client for the Msg objects
    mock_client = MagicMock(spec=NATS)

    test_cases = [
        {
            "name": "valid message",
            "msg": Msg(
                _client=mock_client,
                subject="test",
                data=b"test-data",
                headers={
                    "Nats-Num-Pending": "1",
                    "Nats-Stream": "test-stream",
                    "Nats-Sequence": "1",
                    "Nats-Time-Stamp": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "Nats-Subject": "test-subject",
                },
            ),
            "with_err": None,
        },
        {
            "name": "no messages",
            "msg": Msg(
                _client=mock_client,
                subject="test",
                data=b"",
                headers={
                    "Status": "404",
                },
            ),
            "with_err": jetstreamext.NoMessagesError,
        },
        {
            "name": "missing headers",
            "msg": Msg(
                _client=mock_client,
                subject="test",
                data=b"test-data",
                headers=None,
            ),
            "with_err": jetstreamext.InvalidResponseError,
        },
        {
            "name": "missing Nats-Num-Pending header",
            "msg": Msg(
                _client=mock_client,
                subject="test",
                data=b"test-data",
                headers={
                    "Nats-Stream": "test-stream",
                    "Nats-Sequence": "1",
                    "Nats-Time-Stamp": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "Nats-Subject": "test-subject",
                },
            ),
            "with_err": jetstreamext.BatchUnsupportedError,
        },
        {
            "name": "missing stream header",
            "msg": Msg(
                _client=mock_client,
                subject="test",
                data=b"test-data",
                headers={
                    "Nats-Num-Pending": "1",
                    "Nats-Sequence": "1",
                    "Nats-Time-Stamp": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "Nats-Subject": "test-subject",
                },
            ),
            "with_err": jetstreamext.InvalidResponseError,
        },
        {
            "name": "missing sequence header",
            "msg": Msg(
                _client=mock_client,
                subject="test",
                data=b"test-data",
                headers={
                    "Nats-Num-Pending": "1",
                    "Nats-Stream": "test-stream",
                    "Nats-Time-Stamp": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "Nats-Subject": "test-subject",
                },
            ),
            "with_err": jetstreamext.InvalidResponseError,
        },
        {
            "name": "invalid sequence header",
            "msg": Msg(
                _client=mock_client,
                subject="test",
                data=b"test-data",
                headers={
                    "Nats-Num-Pending": "1",
                    "Nats-Stream": "test-stream",
                    "Nats-Sequence": "invalid-sequence",
                    "Nats-Time-Stamp": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "Nats-Subject": "test-subject",
                },
            ),
            "with_err": jetstreamext.InvalidResponseError,
        },
        {
            "name": "missing timestamp header",
            "msg": Msg(
                _client=mock_client,
                subject="test",
                data=b"test-data",
                headers={
                    "Nats-Num-Pending": "1",
                    "Nats-Stream": "test-stream",
                    "Nats-Sequence": "1",
                    "Nats-Subject": "test-subject",
                },
            ),
            "with_err": jetstreamext.InvalidResponseError,
        },
        {
            "name": "invalid timestamp header",
            "msg": Msg(
                _client=mock_client,
                subject="test",
                data=b"test-data",
                headers={
                    "Nats-Num-Pending": "1",
                    "Nats-Stream": "test-stream",
                    "Nats-Sequence": "1",
                    "Nats-Time-Stamp": "invalid-timestamp",
                    "Nats-Subject": "test-subject",
                },
            ),
            "with_err": jetstreamext.InvalidResponseError,
        },
        {
            "name": "missing subject header",
            "msg": Msg(
                _client=mock_client,
                subject="test",
                data=b"test-data",
                headers={
                    "Nats-Num-Pending": "1",
                    "Nats-Stream": "test-stream",
                    "Nats-Sequence": "1",
                    "Nats-Time-Stamp": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                },
            ),
            "with_err": jetstreamext.InvalidResponseError,
        },
    ]

    for test_case in test_cases:
        print(f"Running test: {test_case['name']}")

        if test_case["with_err"] is not None:
            with pytest.raises(test_case["with_err"]):
                jetstreamext.getbatch._convert_direct_get_msg_response_to_msg(
                    test_case["msg"]
                )
        else:
            # Should not raise an error
            result = jetstreamext.getbatch._convert_direct_get_msg_response_to_msg(
                test_case["msg"]
            )
            assert result is not None


@pytest.mark.asyncio
async def test_get_prefixed_subject(nats_client: NATS):
    """Test getPrefixedSubject function (port of Go TestGetPrefixedSubject)"""

    test_cases = [
        {
            "name": "with APIPrefix without dot",
            "js_opts": {"prefix": "API"},
            "subject": "DIRECT.GET.TEST",
            "expected": "API.DIRECT.GET.TEST",
        },
        {
            "name": "with APIPrefix with dot",
            "js_opts": {"prefix": "API."},
            "subject": "DIRECT.GET.TEST",
            "expected": "API.DIRECT.GET.TEST",
        },
        {
            "name": "with Domain",
            "js_opts": {"domain": "DOMAIN"},
            "subject": "DIRECT.GET.TEST",
            "expected": "$JS.DOMAIN.API.DIRECT.GET.TEST",
        },
        {
            "name": "default prefix",
            "js_opts": {},
            "subject": "DIRECT.GET.TEST",
            "expected": "$JS.API.DIRECT.GET.TEST",
        },
    ]

    for test_case in test_cases:
        print(f"Running test: {test_case['name']}")

        # Create JetStream context with specific options
        js = nats_client.jetstream(**test_case["js_opts"])

        # Test the prefix function
        result = jetstreamext.getbatch._get_prefixed_subject(js, test_case["subject"])
        assert result == test_case["expected"], (
            f"Test '{test_case['name']}': Expected '{test_case['expected']}', got '{result}'"
        )

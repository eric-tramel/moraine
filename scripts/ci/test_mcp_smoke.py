#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mcp_smoke


def handled_error_response() -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "id": 7,
        "result": {
            "content": [{"type": "text", "text": "not found"}],
            "structuredContent": {
                "schema_version": "moraine.mcp.error.v1",
                "tool": "open",
                "request": {"id": "session:missing"},
                "error": {"code": "not_found", "message": "not found"},
            },
            "isError": True,
        },
    }


class ToolResultAssertionTests(unittest.TestCase):
    def test_success_requires_explicit_false_error_flag(self) -> None:
        result = {"isError": False, "structuredContent": {}}
        self.assertIs(mcp_smoke.assert_tool_success(result), result)

        for invalid in ({"isError": True}, {}, {"isError": None}):
            with self.subTest(invalid=invalid):
                with self.assertRaises(AssertionError):
                    mcp_smoke.assert_tool_success(invalid)

    def test_expected_handled_error_validates_response_and_contract(self) -> None:
        response = handled_error_response()
        with mock.patch.object(mcp_smoke, "send_request", return_value=response) as send:
            payload = mcp_smoke.call_tool_expect_handled_error(
                object(), 7, "open", {"id": "session:missing"}, "not_found"
            )

        self.assertEqual(payload["error"]["code"], "not_found")
        sent = send.call_args.args[1]
        self.assertEqual(sent["jsonrpc"], "2.0")
        self.assertEqual(sent["id"], 7)
        self.assertEqual(sent["method"], "tools/call")
        self.assertEqual(sent["params"]["name"], "open")

    def test_expected_handled_error_rejects_json_rpc_error(self) -> None:
        response = {"jsonrpc": "2.0", "id": 7, "error": {"code": -32603}}
        with mock.patch.object(mcp_smoke, "send_request", return_value=response):
            with self.assertRaisesRegex(AssertionError, "rpc error"):
                mcp_smoke.call_tool_expect_handled_error(
                    object(), 7, "open", {"id": "session:missing"}, "not_found"
                )

    def test_expected_handled_error_rejects_contract_mismatches(self) -> None:
        cases = (
            ("isError", False, "isError=true"),
            ("schema_version", "moraine.mcp.open.v1", "schema_version"),
            ("tool", "search_sessions", "wrong tool"),
            ("code", "invalid_id", "must return not_found"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                response = handled_error_response()
                result = response["result"]
                assert isinstance(result, dict)
                structured = result["structuredContent"]
                assert isinstance(structured, dict)
                if field == "isError":
                    result[field] = value
                elif field == "code":
                    error = structured["error"]
                    assert isinstance(error, dict)
                    error[field] = value
                else:
                    structured[field] = value

                with mock.patch.object(mcp_smoke, "send_request", return_value=response):
                    with self.assertRaisesRegex(AssertionError, message):
                        mcp_smoke.call_tool_expect_handled_error(
                            object(),
                            7,
                            "open",
                            {"id": "session:missing"},
                            "not_found",
                        )

    def test_expected_handled_error_rejects_missing_envelope_objects(self) -> None:
        for field, value, message in (
            ("structuredContent", None, "structuredContent missing"),
            ("error", None, "must return not_found"),
        ):
            with self.subTest(field=field):
                response = handled_error_response()
                result = response["result"]
                assert isinstance(result, dict)
                if field == "structuredContent":
                    result[field] = value
                else:
                    structured = result["structuredContent"]
                    assert isinstance(structured, dict)
                    structured[field] = value

                with mock.patch.object(mcp_smoke, "send_request", return_value=response):
                    with self.assertRaisesRegex(AssertionError, message):
                        mcp_smoke.call_tool_expect_handled_error(
                            object(),
                            7,
                            "open",
                            {"id": "session:missing"},
                            "not_found",
                        )


if __name__ == "__main__":
    unittest.main()

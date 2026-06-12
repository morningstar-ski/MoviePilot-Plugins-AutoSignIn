from datetime import datetime, timedelta

import pytest
import pytz

from tests._bootstrap import load_autosignin_captcha_module, load_autosignin_module


def _collect_models(node):
    models = set()
    if isinstance(node, dict):
        props = node.get("props")
        if isinstance(props, dict) and props.get("model"):
            models.add(props["model"])
        for value in node.values():
            models.update(_collect_models(value))
    elif isinstance(node, list):
        for item in node:
            models.update(_collect_models(item))
    return models


def _build_test_plugin(plugin_cls):
    class _SiteOperStub:
        @staticmethod
        def list_order_by_pri():
            return []

    class TestAutoSignIn(plugin_cls):
        def __init__(self):
            self._data = {}
            self.messages = []
            self.updated_config = None
            self._enabled = True
            self._notify = True
            self._queue_cnt = 1
            self._sign_sites = ["1"]
            self._login_sites = ["2"]
            self._clean = False
            self._final_check_time = "22:00"
            self._schedule_state_key = "schedule_state_v2"
            self._random_begin_hour = 12
            self._random_end_hour = 18
            self._retry_min_minutes = 15
            self._retry_max_minutes = 20
            self._captcha_provider = "moviepilot"
            self._captcha_api_key = ""
            self._captcha_api_base_url = ""
            self._captcha_task_type = ""
            self._captcha_model = ""
            self._captcha_timeout = 90
            self.siteoper = _SiteOperStub()
            self._site_map = {
                "1": {"id": "1", "name": "Alpha"},
                "2": {"id": "2", "name": "Beta"},
            }
            self._signin_results = {}
            self._login_results = {}

        def get_data(self, key=None):
            return self._data.get(key)

        def save_data(self, key=None, value=None):
            self._data[key] = value

        def update_config(self, config):
            self.updated_config = config

        def get_config(self, _key):
            return None

        def post_message(self, **kwargs):
            self.messages.append(kwargs)

        def _get_site_map(self):
            return dict(self._site_map)

        def _wrap_signin_site(self, site_info):
            site_id = str(site_info["id"])
            result = self._signin_results.get(
                site_id,
                (site_info["name"], "签到成功", True),
            )
            return site_id, result[0], result[1], result[2]

        def _wrap_login_site(self, site_info):
            site_id = str(site_info["id"])
            result = self._login_results.get(
                site_id,
                (site_info["name"], "模拟登录成功", True),
            )
            return site_id, result[0], result[1], result[2]

    return TestAutoSignIn()


@pytest.mark.parametrize("generation", ["v1", "v2"])
def test_form_contains_new_schedule_and_captcha_fields(generation):
    module = load_autosignin_module(generation)
    plugin = _build_test_plugin(module.AutoSignIn)

    form, defaults = plugin.get_form()
    models = _collect_models(form)

    assert "final_check_time" in models
    assert "captcha_provider" in models
    assert "captcha_api_key" in models
    assert "captcha_api_base_url" in models
    assert "captcha_task_type" in models
    assert "captcha_model" in models
    assert "captcha_timeout" in models

    assert defaults["final_check_time"] == "22:00"
    assert defaults["captcha_provider"] == "moviepilot"
    assert defaults["captcha_timeout"] == 90


@pytest.mark.parametrize("generation", ["v1", "v2"])
def test_daily_schedule_uses_random_window_and_final_check_time(generation):
    module = load_autosignin_module(generation)
    plugin = _build_test_plugin(module.AutoSignIn)
    tz = pytz.timezone(module.settings.TZ)
    now = tz.localize(datetime(2026, 6, 12, 10, 0, 0))

    state = plugin._build_schedule_state(now)
    signin_at = plugin._from_iso(state["tasks"]["signin"]["planned_at"])
    login_at = plugin._from_iso(state["tasks"]["login"]["planned_at"])
    final_check_at = plugin._from_iso(state["final_check_at"])

    assert 12 <= signin_at.hour <= 18
    assert 12 <= login_at.hour <= 18
    assert final_check_at.hour == 22
    assert final_check_at.minute == 0


@pytest.mark.parametrize("generation", ["v1", "v2"])
def test_failed_run_sets_retry_window_and_becomes_due(generation):
    module = load_autosignin_module(generation)
    plugin = _build_test_plugin(module.AutoSignIn)
    tz = pytz.timezone(module.settings.TZ)
    now = tz.localize(datetime(2026, 6, 12, 12, 30, 0))
    plugin._sign_sites = ["1"]
    plugin._signin_results["1"] = ("Alpha", "签到失败", False)

    state = plugin._build_schedule_state(now)
    task_state = state["tasks"]["signin"]
    task_state["planned_at"] = plugin._to_iso(now - timedelta(minutes=1))

    plugin._run_task("signin", ["1"], state, now, reason="scheduled")

    site_runtime = task_state["sites"]["1"]
    retry_at = plugin._from_iso(site_runtime["next_retry_at"])
    retry_delta = (retry_at - now).total_seconds() / 60

    assert site_runtime["success"] is False
    assert site_runtime["attempts"] == 1
    assert 15 <= retry_delta <= 20

    due_before, reason_before = plugin._get_due_site_ids(
        state, "signin", retry_at - timedelta(minutes=1)
    )
    due_after, reason_after = plugin._get_due_site_ids(state, "signin", retry_at)

    assert due_before == []
    assert reason_before is None
    assert due_after == ["1"]
    assert reason_after == "retry"


@pytest.mark.parametrize("generation", ["v1", "v2"])
def test_final_check_notifies_when_sites_still_pending(generation):
    module = load_autosignin_module(generation)
    plugin = _build_test_plugin(module.AutoSignIn)
    tz = pytz.timezone(module.settings.TZ)
    now = tz.localize(datetime(2026, 6, 12, 22, 0, 0))
    plugin._signin_results["1"] = ("Alpha", "签到失败", False)
    plugin._login_results["2"] = ("Beta", "模拟登录成功", True)

    state = plugin._build_schedule_state(now)
    state["tasks"]["signin"]["sites"]["1"]["success"] = False
    state["tasks"]["login"]["sites"]["2"]["success"] = False

    plugin._run_final_check(state, now)

    assert state["final_check_done"] is True
    assert plugin.messages
    assert "22:00" in plugin.messages[-1]["text"]


@pytest.mark.parametrize("generation", ["v1", "v2"])
def test_captcha_provider_defaults_and_fallbacks(generation):
    captcha_module = load_autosignin_captcha_module(generation)
    solver = captcha_module.CaptchaSolver

    moviepilot = solver._normalize_config({"provider": "moviepilot", "timeout": "9"})
    yescaptcha = solver._normalize_config({"provider": "yescaptcha"})
    capsolver = solver._normalize_config({"provider": "capsolver"})
    custom = solver._normalize_config({"provider": "custom", "api_base_url": "https://example.com/api"})
    fallback = solver._normalize_config({"provider": "unknown"})

    assert moviepilot["timeout"] == 15
    assert yescaptcha["api_base_url"] == "https://api.yescaptcha.com"
    assert yescaptcha["task_type"] == "ImageToTextTaskMuggle"
    assert capsolver["api_base_url"] == "https://api.capsolver.com"
    assert capsolver["model"] == "common"
    assert custom["api_base_url"] == "https://example.com/api"
    assert fallback["provider"] == "moviepilot"
    assert solver._resolve_api_url("https://api.yescaptcha.com", "createTask") == "https://api.yescaptcha.com/createTask"

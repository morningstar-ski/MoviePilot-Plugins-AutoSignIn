import base64
import time
from typing import Any, Dict, Optional

from app.core.config import settings
from app.helper.ocr import OcrHelper
from app.log import logger
from app.utils.http import RequestUtils


class CaptchaSolver:
    _PROVIDER_DEFAULTS = {
        "moviepilot": {
            "base_url": "",
            "task_type": "",
            "model": "",
        },
        "yescaptcha": {
            "base_url": "https://api.yescaptcha.com",
            "task_type": "ImageToTextTaskMuggle",
            "model": "",
        },
        "capsolver": {
            "base_url": "https://api.capsolver.com",
            "task_type": "ImageToTextTask",
            "model": "common",
        },
        "twocaptcha": {
            "base_url": "https://api.2captcha.com",
            "task_type": "ImageToTextTask",
            "model": "",
        },
        "anticaptcha": {
            "base_url": "https://api.anti-captcha.com",
            "task_type": "ImageToTextTask",
            "model": "",
        },
        "custom": {
            "base_url": "",
            "task_type": "ImageToTextTask",
            "model": "",
        },
    }
    _config: Dict[str, Any] = {
        "provider": "moviepilot",
        "api_key": "",
        "api_base_url": "",
        "task_type": "",
        "model": "",
        "timeout": 90,
    }

    @classmethod
    def configure(cls, config: Optional[dict] = None):
        cls._config = cls._normalize_config(config or {})

    @classmethod
    def solve(
        cls,
        image_url: Optional[str] = None,
        image_b64: Optional[str] = None,
        cookie: Optional[str] = None,
        ua: Optional[str] = None,
        proxy: bool = False,
        website_url: Optional[str] = None,
    ) -> str:
        config = cls._normalize_config(cls._config)
        return cls._solve_with_config(
            config=config,
            image_url=image_url,
            image_b64=image_b64,
            cookie=cookie,
            ua=ua,
            proxy=proxy,
            website_url=website_url,
        )

    @classmethod
    def _solve_with_config(
        cls,
        config: dict,
        image_url: Optional[str],
        image_b64: Optional[str],
        cookie: Optional[str],
        ua: Optional[str],
        proxy: bool,
        website_url: Optional[str],
    ) -> str:
        provider = config.get("provider") or "moviepilot"
        timeout = int(config.get("timeout") or 90)
        image_b64 = image_b64 or cls._download_image_base64(
            image_url=image_url,
            cookie=cookie,
            ua=ua,
            proxy=proxy,
            timeout=timeout,
        )
        if not image_b64:
            return ""

        if provider == "moviepilot":
            return (OcrHelper().get_captcha_text(image_b64=image_b64) or "").strip()

        api_key = str(config.get("api_key") or "").strip()
        if not api_key:
            logger.warn(f"Captcha provider {provider} missing API key")
            return ""

        base_url = str(config.get("api_base_url") or "").strip()
        task_type = str(config.get("task_type") or "").strip()
        model = str(config.get("model") or "").strip()
        task_payload = {
            "type": task_type or cls._default_task_type(provider),
            "body": image_b64,
        }
        if website_url:
            task_payload["websiteURL"] = website_url

        if provider == "capsolver":
            task_payload["module"] = model or cls._PROVIDER_DEFAULTS["capsolver"]["model"]
        elif model and provider in ["yescaptcha", "custom"]:
            task_payload["type"] = model

        create_payload = {
            "clientKey": api_key,
            "task": task_payload,
        }

        if provider in ["twocaptcha", "anticaptcha"]:
            create_payload["languagePool"] = "en"

        return cls._create_and_resolve(
            provider=provider,
            base_url=base_url,
            api_key=api_key,
            create_payload=create_payload,
            timeout=timeout,
        )

    @classmethod
    def _create_and_resolve(
        cls,
        provider: str,
        base_url: str,
        api_key: str,
        create_payload: dict,
        timeout: int,
    ) -> str:
        create_url = cls._resolve_api_url(base_url, "createTask")
        get_url = cls._resolve_api_url(base_url, "getTaskResult")
        if not create_url:
            logger.error(f"Captcha provider {provider} missing API endpoint")
            return ""

        response = cls._post_json(create_url, create_payload, timeout)
        if not response:
            return ""

        if cls._has_error(response):
            logger.error(f"Captcha provider {provider} createTask failed: {cls._format_error(response)}")
            return ""

        direct_text = cls._extract_solution_text(response)
        if direct_text:
            return direct_text

        task_id = response.get("taskId")
        if not task_id or not get_url:
            logger.error(f"Captcha provider {provider} did not return a usable result")
            return ""

        started_at = time.time()
        while time.time() - started_at < timeout:
            time.sleep(3)
            result = cls._post_json(get_url, {"clientKey": api_key, "taskId": task_id}, timeout)
            if not result:
                continue
            if cls._has_error(result):
                logger.error(f"Captcha provider {provider} getTaskResult failed: {cls._format_error(result)}")
                return ""
            if str(result.get("status") or "").lower() in ["processing", "queued"]:
                continue
            text = cls._extract_solution_text(result)
            if text:
                return text
            logger.error(f"Captcha provider {provider} returned no text: {result}")
            return ""

        logger.error(f"Captcha provider {provider} timed out after {timeout}s")
        return ""

    @staticmethod
    def _post_json(url: str, payload: dict, timeout: int) -> Optional[dict]:
        res = RequestUtils(content_type="application/json", timeout=timeout).post_res(url=url, json=payload)
        if not res:
            return None
        try:
            return res.json()
        except Exception as err:
            logger.error(f"Captcha response parse failed: {err}")
            return None

    @staticmethod
    def _download_image_base64(
        image_url: Optional[str],
        cookie: Optional[str],
        ua: Optional[str],
        proxy: bool,
        timeout: int,
    ) -> str:
        if not image_url:
            return ""
        res = RequestUtils(
            ua=ua,
            cookies=cookie,
            proxies=settings.PROXY if proxy else None,
            timeout=timeout,
        ).get_res(image_url)
        if not res or not res.content:
            return ""
        return base64.b64encode(res.content).decode()

    @classmethod
    def _normalize_config(cls, config: dict) -> dict:
        provider = str(config.get("provider") or "moviepilot").strip().lower()
        if provider not in cls._PROVIDER_DEFAULTS:
            provider = "moviepilot"
        defaults = cls._PROVIDER_DEFAULTS[provider]
        timeout = config.get("timeout") or 90
        try:
            timeout = max(15, int(timeout))
        except Exception:
            timeout = 90
        return {
            "provider": provider,
            "api_key": str(config.get("api_key") or "").strip(),
            "api_base_url": str(config.get("api_base_url") or defaults.get("base_url") or "").strip(),
            "task_type": str(config.get("task_type") or defaults.get("task_type") or "").strip(),
            "model": str(config.get("model") or defaults.get("model") or "").strip(),
            "timeout": timeout,
        }

    @classmethod
    def _default_task_type(cls, provider: str) -> str:
        return cls._PROVIDER_DEFAULTS.get(provider, {}).get("task_type") or "ImageToTextTask"

    @staticmethod
    def _resolve_api_url(base_url: str, action: str) -> str:
        base_url = str(base_url or "").strip()
        if not base_url:
            return ""
        if base_url.endswith(f"/{action}"):
            return base_url
        if base_url.endswith("/"):
            return f"{base_url}{action}"
        return f"{base_url}/{action}"

    @staticmethod
    def _has_error(response: dict) -> bool:
        error_id = response.get("errorId")
        if error_id not in [None, 0, "0"]:
            return True
        error_code = str(response.get("errorCode") or "").strip()
        status = str(response.get("status") or "").strip().lower()
        return bool(error_code and status not in ["ready", "processing", "queued"])

    @staticmethod
    def _format_error(response: dict) -> str:
        return str(
            response.get("errorDescription")
            or response.get("errorCode")
            or response.get("message")
            or response
        )

    @staticmethod
    def _extract_solution_text(response: dict) -> str:
        solution = response.get("solution")
        if isinstance(solution, dict):
            for key in ["text", "token", "gRecaptchaResponse"]:
                value = solution.get(key)
                if value:
                    return str(value).strip()
        for key in ["text", "solution", "result"]:
            value = response.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

import random
import re
import traceback
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing.pool import ThreadPool
from threading import Lock
from typing import Any, List, Dict, Tuple, Optional
from urllib.parse import urljoin

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from ruamel.yaml import CommentedMap

from app import schemas
from app.chain.site import SiteChain
from app.core.config import settings
from app.core.event import EventManager, eventmanager, Event
from app.db.site_oper import SiteOper
from app.db.sitestatistic_oper import SiteStatisticOper
from app.helper.browser import PlaywrightHelper
from app.helper.cloudflare import under_challenge
from app.helper.module import ModuleHelper
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType
from app.utils.http import RequestUtils
from app.utils.site import SiteUtils
from app.utils.string import StringUtils

from .captcha import CaptchaSolver


class AutoSignIn(_PluginBase):
    # 插件名称
    plugin_name = "站点自动签到"
    # 插件描述
    plugin_desc = "自动模拟登录、签到站点。"
    # 插件图标
    plugin_icon = "signin.png"
    # 插件版本
    plugin_version = "2.9.4"
    # 插件作者
    plugin_author = "thsrite"
    # 作者主页
    author_url = "https://github.com/thsrite"
    # 插件配置项ID前缀
    plugin_config_prefix = "autosignin_"
    # 加载顺序
    plugin_order = 0
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    sites: SitesHelper = None
    siteoper: SiteOper = None
    sitechain: SiteChain = None
    sitestatistic: SiteStatisticOper = None
    # 事件管理器
    event: EventManager = None
    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None
    # 加载的模块
    _site_schema: list = []

    # 配置属性
    _enabled: bool = False
    _cron: str = ""
    _onlyonce: bool = False
    _notify: bool = False
    _queue_cnt: int = 5
    _sign_sites: list = []
    _login_sites: list = []
    _retry_keyword = None
    _clean: bool = False
    _start_time: int = None
    _end_time: int = None
    _auto_cf: int = 0
    _final_check_time: str = "22:00"
    _schedule_lock: Lock = Lock()
    _schedule_state_key = "schedule_state_v2"
    _random_begin_hour = 12
    _random_end_hour = 18
    _retry_min_minutes = 15
    _retry_max_minutes = 20
    _startup_reconcile_delay_seconds = 15
    _captcha_provider: str = "moviepilot"
    _captcha_api_key: str = ""
    _captcha_api_base_url: str = ""
    _captcha_task_type: str = ""
    _captcha_model: str = ""
    _captcha_timeout: int = 90

    def init_plugin(self, config: dict = None):
        self.sites = SitesHelper()
        self.siteoper = SiteOper()
        self.event = EventManager()
        self.sitechain = SiteChain()
        self.sitestatistic = SiteStatisticOper()

        # 停止现有任务
        self.stop_service()

        # 配置
        if config:
            self._enabled = config.get("enabled")
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce")
            self._notify = config.get("notify")
            self._queue_cnt = config.get("queue_cnt") or 5
            self._sign_sites = config.get("sign_sites") or []
            self._login_sites = config.get("login_sites") or []
            self._retry_keyword = config.get("retry_keyword")
            self._auto_cf = config.get("auto_cf")
            self._clean = config.get("clean")
            self._final_check_time = config.get("final_check_time") or "22:00"
            self._captcha_provider = config.get("captcha_provider") or "moviepilot"
            self._captcha_api_key = config.get("captcha_api_key") or ""
            self._captcha_api_base_url = config.get("captcha_api_base_url") or ""
            self._captcha_task_type = config.get("captcha_task_type") or ""
            self._captcha_model = config.get("captcha_model") or ""
            self._captcha_timeout = config.get("captcha_timeout") or 90

            # 过滤掉已删除的站点
            all_sites = [site.id for site in self.siteoper.list_order_by_pri()] + [site.get("id") for site in
                                                                                   self.__custom_sites()]
            self._sign_sites = [site_id for site_id in all_sites if site_id in self._sign_sites]
            self._login_sites = [site_id for site_id in all_sites if site_id in self._login_sites]
            # 保存配置
            self.__update_config()

        # 加载模块
        if self._enabled or self._onlyonce:
            self._configure_captcha_solver()

            self._site_schema = ModuleHelper.load('app.plugins.autosignin.sites',
                                                  filter_func=lambda _, obj: hasattr(obj, 'match'))

            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._enabled:
                logger.info("站点自动签到服务启动，15 秒后执行一次启动巡检补偿")
                self._scheduler.add_job(func=self.service_tick,
                                        trigger='date',
                                        run_date=self._now() + timedelta(seconds=self._startup_reconcile_delay_seconds),
                                        id="AutoSignInStartupTick",
                                        name="站点自动签到启动巡检",
                                        replace_existing=True)

            # 立即运行一次
            if self._onlyonce:
                logger.info("站点自动签到服务启动，立即运行一次")
                self._scheduler.add_job(func=self.sign_in, trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        id="AutoSignInOnlyOnce",
                                        name="站点自动签到",
                                        replace_existing=True)

                # 关闭一次性开关
                self._onlyonce = False
                # 保存配置
                self.__update_config()

                # 启动任务
            if self._scheduler and self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    def __update_config(self):
        # 保存配置
        self.update_config(
            {
                "enabled": self._enabled,
                "notify": self._notify,
                "cron": self._cron,
                "onlyonce": self._onlyonce,
                "queue_cnt": self._queue_cnt,
                "sign_sites": self._sign_sites,
                "login_sites": self._login_sites,
                "retry_keyword": self._retry_keyword,
                "auto_cf": self._auto_cf,
                "clean": self._clean,
                "final_check_time": self._final_check_time,
                "captcha_provider": self._captcha_provider,
                "captcha_api_key": self._captcha_api_key,
                "captcha_api_base_url": self._captcha_api_base_url,
                "captcha_task_type": self._captcha_task_type,
                "captcha_model": self._captcha_model,
                "captcha_timeout": self._captcha_timeout,
            }
        )

    def _configure_captcha_solver(self):
        CaptchaSolver.configure(
            {
                "provider": self._captcha_provider,
                "api_key": self._captcha_api_key,
                "api_base_url": self._captcha_api_base_url,
                "task_type": self._captcha_task_type,
                "model": self._captcha_model,
                "timeout": self._captcha_timeout,
            }
        )

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        :return: 命令关键字、事件、描述、附带数据
        """
        return [{
            "cmd": "/site_signin",
            "event": EventType.PluginAction,
            "desc": "站点签到",
            "category": "站点",
            "data": {
                "action": "site_signin"
            }
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件API
        [{
            "path": "/xx",
            "endpoint": self.xxx,
            "methods": ["GET", "POST"],
            "summary": "API说明"
        }]
        """
        return [{
            "path": "/signin_by_domain",
            "endpoint": self.signin_by_domain,
            "methods": ["GET"],
            "summary": "站点签到",
            "description": "使用站点域名签到站点",
        }]

    def get_service(self) -> List[Dict[str, Any]]:
        return [{
            "id": "AutoSignInHeartbeat",
            "name": "站点自动签到巡检服务",
            "trigger": "interval",
            "func": self.service_tick,
            "kwargs": {
                "minutes": 1
            }
        }] if self._enabled else []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        # 站点的可选项（内置站点 + 自定义站点）
        customSites = self.__custom_sites()

        site_options = ([{"title": site.name, "value": site.id}
                         for site in self.siteoper.list_order_by_pri()]
                        + [{"title": site.get("name"), "value": site.get("id")}
                           for site in customSites])
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'clean',
                                            'label': '清理本日缓存',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'final_check_time',
                                            'label': '最终检查时间',
                                            'placeholder': '默认 22:00，格式 HH:MM'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'queue_cnt',
                                            'label': '队列数量'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'retry_keyword',
                                            'label': '重试关键词(兼容保留)',
                                            'placeholder': '当前版本默认所有失败都会随机 15-20 分钟重试'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'auto_cf',
                                            'label': '自动优选',
                                            'placeholder': '命中重试关键词次数（0-关闭）'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'captcha_provider',
                                            'label': '验证码识别',
                                            'items': [
                                                {'title': 'MoviePilot 免费OCR', 'value': 'moviepilot'},
                                                {'title': 'YesCaptcha', 'value': 'yescaptcha'},
                                                {'title': 'CapSolver', 'value': 'capsolver'},
                                                {'title': '2Captcha', 'value': 'twocaptcha'},
                                                {'title': 'Anti-Captcha', 'value': 'anticaptcha'},
                                                {'title': '自定义兼容接口', 'value': 'custom'}
                                            ]
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'captcha_api_key',
                                            'label': '验证码API Key',
                                            'type': 'password',
                                            'placeholder': '默认 MoviePilot 免费OCR 可留空'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'captcha_api_base_url',
                                            'label': '验证码API地址',
                                            'placeholder': '留空使用预置官方地址，自定义兼容接口时必填'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'captcha_task_type',
                                            'label': '验证码任务类型',
                                            'placeholder': '高级项，留空使用预置默认值'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'captcha_model',
                                            'label': '验证码模型',
                                            'placeholder': 'Capsolver 可填 common'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'captcha_timeout',
                                            'label': '验证码超时(秒)'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'chips': True,
                                            'multiple': True,
                                            'model': 'sign_sites',
                                            'label': '签到站点',
                                            'items': site_options
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'chips': True,
                                            'multiple': True,
                                            'model': 'login_sites',
                                            'label': '登录站点',
                                            'items': site_options
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '每天 12:00-18:59 之间为签到和模拟登录各生成一次随机主任务；失败后会按 15-20 分钟随机重试直到成功；最终检查时间默认 22:00，可单独配置。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '验证码识别默认走 MoviePilot 免费OCR；也可切换 YesCaptcha、CapSolver、2Captcha、Anti-Captcha 或自定义兼容 createTask/getTaskResult 接口。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'warning',
                                            'variant': 'tonal',
                                            'text': '不是所有的站点都会把程序自动登录/签到定义为用户活跃（比如馒头），提示签到/登录成功仍然存在掉号风险！请结合站点公告说明自行把握。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": True,
            "cron": "",
            "final_check_time": "22:00",
            "auto_cf": 0,
            "onlyonce": False,
            "clean": False,
            "captcha_provider": "moviepilot",
            "captcha_api_key": "",
            "captcha_api_base_url": "",
            "captcha_task_type": "",
            "captcha_model": "",
            "captcha_timeout": 90,
            "queue_cnt": 5,
            "sign_sites": [],
            "login_sites": [],
            "retry_keyword": "错误|失败"
        }

    def __custom_sites(self) -> List[Any]:
        custom_sites = []
        custom_sites_config = self.get_config("CustomSites")
        if custom_sites_config and custom_sites_config.get("enabled"):
            custom_sites = custom_sites_config.get("sites")
        return custom_sites

    def get_page(self) -> List[dict]:
        """
        拼装插件详情页面，需要返回页面配置，同时附带数据
        """
        # 最近两天的日期数组
        date_list = [(datetime.now() - timedelta(days=i)).date() for i in range(2)]
        # 最近一天的签到数据
        current_day = ""
        sign_data = []
        for day in date_list:
            current_day = f"{day.month}月{day.day}日"
            sign_data = self.get_data(current_day)
            if sign_data:
                break
        if sign_data:
            contents = [
                {
                    'component': 'tr',
                    'props': {
                        'class': 'text-sm'
                    },
                    'content': [
                        {
                            'component': 'td',
                            'props': {
                                'class': 'whitespace-nowrap break-keep text-high-emphasis'
                            },
                            'text': current_day
                        },
                        {
                            'component': 'td',
                            'text': data.get("site")
                        },
                        {
                            'component': 'td',
                            'text': data.get("status")
                        }
                    ]
                } for data in sign_data
            ]
        else:
            contents = [
                {
                    'component': 'tr',
                    'props': {
                        'class': 'text-sm'
                    },
                    'content': [
                        {
                            'component': 'td',
                            'props': {
                                'colspan': 3,
                                'class': 'text-center'
                            },
                            'text': '暂无数据'
                        }
                    ]
                }
            ]
        return [
            {
                'component': 'VTable',
                'props': {
                    'hover': True
                },
                'content': [
                    {
                        'component': 'thead',
                        'content': [
                            {
                                'component': 'th',
                                'props': {
                                    'class': 'text-start ps-4'
                                },
                                'text': '日期'
                            },
                            {
                                'component': 'th',
                                'props': {
                                    'class': 'text-start ps-4'
                                },
                                'text': '站点'
                            },
                            {
                                'component': 'th',
                                'props': {
                                    'class': 'text-start ps-4'
                                },
                                'text': '状态'
                            }
                        ]
                    },
                    {
                        'component': 'tbody',
                        'content': contents
                    }
                ]
            }
        ]

    @eventmanager.register(EventType.PluginAction)
    def sign_in(self, event: Event = None):
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "site_signin":
                return

        if not self._schedule_lock.acquire(blocking=False):
            logger.warn("站点自动签到任务正在执行，本次请求跳过")
            return

        try:
            now = self._now()
            state = self._ensure_schedule_state(now)

            if event:
                logger.info("收到命令，开始站点签到...")
                self.post_message(channel=event.event_data.get("channel"),
                                  title="开始站点签到...",
                                  userid=event.event_data.get("user"))

            self._run_task(task_key="signin",
                           site_ids=self._normalize_site_ids(self._sign_sites),
                           state=state,
                           now=now,
                           reason="manual")
            self._run_task(task_key="login",
                           site_ids=self._normalize_site_ids(self._login_sites),
                           state=state,
                           now=now,
                           reason="manual")
            self._save_schedule_state(state)

            if event:
                self.post_message(channel=event.event_data.get("channel"),
                                  title="站点签到完成",
                                  userid=event.event_data.get("user"))
        except Exception as err:
            logger.error(f"站点签到任务执行失败：{err}")
            logger.error(traceback.format_exc())
            if event:
                self.post_message(channel=event.event_data.get("channel"),
                                  title="站点签到任务失败",
                                  userid=event.event_data.get("user"))
        finally:
            self._schedule_lock.release()
            self.__update_config()

    def service_tick(self):
        if not self._enabled:
            return

        if not self._schedule_lock.acquire(blocking=False):
            logger.info("站点自动签到任务仍在运行，跳过本次巡检")
            return

        try:
            now = self._now()
            state = self._ensure_schedule_state(now)

            for task_key in ("signin", "login"):
                due_site_ids, reason = self._get_due_site_ids(state, task_key, now)
                if due_site_ids:
                    self._run_task(task_key=task_key,
                                   site_ids=due_site_ids,
                                   state=state,
                                   now=now,
                                   reason=reason)

            final_check_at = self._from_iso(state.get("final_check_at")) or self._parse_final_check_time(now)
            if now >= final_check_at and not state.get("final_check_done"):
                self._run_final_check(state, now)

            self._save_schedule_state(state)
        except Exception as err:
            logger.error(f"站点自动签到巡检失败：{err}")
            logger.error(traceback.format_exc())
        finally:
            self._schedule_lock.release()

    def _now(self) -> datetime:
        return datetime.now(tz=pytz.timezone(settings.TZ))

    @staticmethod
    def _to_iso(dt: Optional[datetime]) -> Optional[str]:
        if not dt:
            return None
        return dt.isoformat(timespec="seconds")

    def _from_iso(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                return pytz.timezone(settings.TZ).localize(dt)
            return dt.astimezone(pytz.timezone(settings.TZ))
        except Exception:
            return None

    @staticmethod
    def _normalize_site_ids(site_ids: Optional[list]) -> List[str]:
        normalized = []
        for site_id in site_ids or []:
            site_id = str(site_id)
            if site_id not in normalized:
                normalized.append(site_id)
        return normalized

    def _get_site_map(self) -> Dict[str, Any]:
        all_sites = [site for site in self.sites.get_indexers() if not site.get("public")] + self.__custom_sites()
        return {str(site.get("id")): site for site in all_sites if site.get("id") is not None}

    def _random_day_time(self, now: datetime, start_hour: int, end_hour: int) -> datetime:
        start_at = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        end_at = now.replace(hour=end_hour, minute=59, second=59, microsecond=0)
        total_minutes = max(int((end_at - start_at).total_seconds() // 60), 0)
        return start_at + timedelta(minutes=random.randint(0, total_minutes))

    @staticmethod
    def _format_clock(dt: Optional[datetime]) -> str:
        if not dt:
            return "-"
        return dt.strftime("%H:%M")

    @staticmethod
    def _format_site_list(site_map: Dict[str, Any], site_ids: list) -> str:
        site_names = []
        for site_id in site_ids or []:
            site_info = site_map.get(site_id) or {}
            site_names.append(site_info.get("name") or str(site_id))
        return ", ".join(site_names) if site_names else "无"

    def _parse_final_check_time(self, now: datetime) -> datetime:
        try:
            hour_str, minute_str = str(self._final_check_time or "22:00").split(":", 1)
            hour = int(hour_str)
            minute = int(minute_str)
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                raise ValueError("invalid final check time")
        except Exception:
            logger.error(f"最终检查时间配置无效：{self._final_check_time}，已回退到 22:00")
            hour = 22
            minute = 0
        return now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    def _build_site_runtime(self, site_info: Any) -> dict:
        return {
            "site_name": site_info.get("name") if site_info else "",
            "success": False,
            "attempts": 0,
            "last_message": "",
            "last_run_at": None,
            "next_retry_at": None,
        }

    def _build_task_state(self, selected_sites: list, label: str, site_map: Dict[str, Any], now: datetime) -> dict:
        selected_site_ids = [site_id for site_id in self._normalize_site_ids(selected_sites) if site_id in site_map]
        return {
            "label": label,
            "planned_at": self._to_iso(self._random_day_time(now,
                                                              self._random_begin_hour,
                                                              self._random_end_hour)),
            "selected_sites": selected_site_ids,
            "sites": {
                site_id: self._build_site_runtime(site_map.get(site_id))
                for site_id in selected_site_ids
            }
        }

    def _sync_task_state(self, task_state: Optional[dict], selected_sites: list, label: str,
                         site_map: Dict[str, Any], now: datetime) -> dict:
        if not isinstance(task_state, dict):
            task_state = self._build_task_state(selected_sites, label, site_map, now)

        selected_site_ids = [site_id for site_id in self._normalize_site_ids(selected_sites) if site_id in site_map]
        planned_at = self._from_iso(task_state.get("planned_at")) or self._random_day_time(now,
                                                                                              self._random_begin_hour,
                                                                                              self._random_end_hour)
        sites_state = task_state.get("sites") or {}
        synced_sites = {}
        for site_id in selected_site_ids:
            site_runtime = sites_state.get(site_id) or self._build_site_runtime(site_map.get(site_id))
            site_runtime["site_name"] = site_map.get(site_id).get("name")
            synced_sites[site_id] = site_runtime

        task_state["label"] = label
        task_state["planned_at"] = self._to_iso(planned_at)
        task_state["selected_sites"] = selected_site_ids
        task_state["sites"] = synced_sites
        return task_state

    def _build_schedule_state(self, now: datetime) -> dict:
        site_map = self._get_site_map()
        state = {
            "date": now.strftime("%Y-%m-%d"),
            "final_check_at": self._to_iso(self._parse_final_check_time(now)),
            "final_check_done": False,
            "tasks": {
                "signin": self._build_task_state(self._sign_sites, "签到", site_map, now),
                "login": self._build_task_state(self._login_sites, "模拟登录", site_map, now),
            }
        }
        signin_at = self._from_iso(state.get("tasks", {}).get("signin", {}).get("planned_at"))
        login_at = self._from_iso(state.get("tasks", {}).get("login", {}).get("planned_at"))
        final_check_at = self._from_iso(state.get("final_check_at"))
        logger.info(f"站点自动签到已生成当日随机计划：签到 {self._format_clock(signin_at)}，"
                    f"模拟登录 {self._format_clock(login_at)}，"
                    f"最终检查 {self._format_clock(final_check_at)}")
        return state

    def _load_schedule_state(self) -> dict:
        state = self.get_data(key=self._schedule_state_key)
        return state if isinstance(state, dict) else {}

    def _save_schedule_state(self, state: dict):
        self.save_data(key=self._schedule_state_key, value=state)

    def _ensure_schedule_state(self, now: datetime) -> dict:
        state = self._load_schedule_state()
        today = now.strftime("%Y-%m-%d")

        if self._clean or state.get("date") != today:
            state = self._build_schedule_state(now)
            if self._clean:
                self._clean = False
            self.__update_config()
            self._save_schedule_state(state)
            return state

        site_map = self._get_site_map()
        state["final_check_at"] = self._to_iso(self._parse_final_check_time(now))
        state["final_check_done"] = bool(state.get("final_check_done"))
        tasks = state.get("tasks") or {}
        state["tasks"] = {
            "signin": self._sync_task_state(tasks.get("signin"), self._sign_sites, "签到", site_map, now),
            "login": self._sync_task_state(tasks.get("login"), self._login_sites, "模拟登录", site_map, now),
        }
        logger.debug(f"站点自动签到巡检状态：日期 {state.get('date')}，最终检查 {self._format_clock(self._from_iso(state.get('final_check_at')))}")
        return state

    def _get_pending_site_ids(self, state: dict, task_key: str) -> List[str]:
        task_state = (state.get("tasks") or {}).get(task_key) or {}
        pending_site_ids = []
        for site_id in task_state.get("selected_sites") or []:
            site_runtime = (task_state.get("sites") or {}).get(site_id) or {}
            if not site_runtime.get("success"):
                pending_site_ids.append(site_id)
        return pending_site_ids

    def _get_due_site_ids(self, state: dict, task_key: str, now: datetime) -> Tuple[List[str], Optional[str]]:
        task_state = (state.get("tasks") or {}).get(task_key) or {}
        planned_at = self._from_iso(task_state.get("planned_at")) or self._random_day_time(now,
                                                                                              self._random_begin_hour,
                                                                                              self._random_end_hour)
        first_run_due = []
        retry_due = []
        for site_id in task_state.get("selected_sites") or []:
            site_runtime = (task_state.get("sites") or {}).get(site_id) or {}
            if site_runtime.get("success"):
                continue

            attempts = int(site_runtime.get("attempts") or 0)
            retry_at = self._from_iso(site_runtime.get("next_retry_at"))
            if attempts == 0 and now >= planned_at:
                first_run_due.append(site_id)
            elif retry_at and now >= retry_at:
                retry_due.append(site_id)

        if first_run_due:
            return first_run_due + [site_id for site_id in retry_due if site_id not in first_run_due], "scheduled"
        if retry_due:
            return retry_due, "retry"
        return [], None

    def _next_retry_time(self, now: datetime) -> datetime:
        return now + timedelta(minutes=random.randint(self._retry_min_minutes, self._retry_max_minutes))

    @staticmethod
    def _is_success_message(task_key: str, message: str) -> bool:
        message = str(message or "")
        if task_key == "signin":
            return any(token in message for token in ["签到成功", "仿真签到成功", "已签到"])
        return "模拟登录成功" in message

    @staticmethod
    def _need_refresh_cookie(message: str) -> bool:
        return "Cookie已失效" in str(message or "")

    def _wrap_signin_site(self, site_info: Any) -> Tuple[str, str, str, bool]:
        site_name, message = self.signin_site(site_info)
        return str(site_info.get("id")), site_name, message, self._is_success_message("signin", message)

    def _wrap_login_site(self, site_info: Any) -> Tuple[str, str, str, bool]:
        site_name, message = self.login_site(site_info)
        return str(site_info.get("id")), site_name, message, self._is_success_message("login", message)

    def _append_daily_log(self, now: datetime, results: list):
        key = f"{now.month}月{now.day}日"
        today_data = self.get_data(key) or []
        if not isinstance(today_data, list):
            today_data = [today_data]
        for _, site_name, message, _ in results:
            today_data.append({
                "site": site_name,
                "status": message
            })
        self.save_data(key, today_data)

    def _save_legacy_task_history(self, task_key: str, state: dict, now: datetime):
        task_state = (state.get("tasks") or {}).get(task_key) or {}
        label = task_state.get("label")
        if not label:
            return
        self.save_data(
            key=f"{label}-{now.strftime('%Y-%m-%d')}",
            value={
                "do": self._normalize_site_ids(task_state.get("selected_sites") or []),
                "retry": self._get_pending_site_ids(state, task_key)
            }
        )

    def _maybe_send_batch_notification(self, task_key: str, reason: str, task_state: dict,
                                       run_site_ids: list, results: list,
                                       unresolved_before: int, unresolved_after: int):
        if reason in ["manual", "final_check"]:
            return
        if not self._notify:
            return
        if reason == "retry" and unresolved_after > 0:
            return

        reason_map = {
            "scheduled": "随机主任务",
            "retry": "失败重试补齐",
        }
        detail_lines = "\n".join([f"【{site_name}】{message}" for _, site_name, message, _ in results])
        self.post_message(title=f"【站点自动{task_state.get('label')}】",
                          mtype=NotificationType.SiteMessage,
                          text=f"执行阶段: {reason_map.get(reason, reason)}\n"
                               f"配置站点数: {len(task_state.get('selected_sites') or [])}\n"
                               f"本次执行数: {len(run_site_ids)}\n"
                               f"剩余未完成: {unresolved_after}\n"
                               f"执行前未完成: {unresolved_before}\n"
                               f"{detail_lines}")

    def _run_task(self, task_key: str, site_ids: list, state: dict, now: datetime, reason: str):
        task_state = (state.get("tasks") or {}).get(task_key) or {}
        site_map = self._get_site_map()
        normalized_site_ids = [site_id for site_id in self._normalize_site_ids(site_ids) if site_id in site_map]
        if not normalized_site_ids:
            return []

        site_infos = [site_map[site_id] for site_id in normalized_site_ids]
        worker = self._wrap_signin_site if task_key == "signin" else self._wrap_login_site
        unresolved_before = len(self._get_pending_site_ids(state, task_key))
        logger.info(f"开始执行站点{task_state.get('label')}任务，原因：{reason}，站点数：{len(site_infos)}，"
                    f"站点：{self._format_site_list(site_map, normalized_site_ids)}")

        queue_size = max(1, int(self._queue_cnt or 1))
        with ThreadPool(min(len(site_infos), queue_size)) as pool:
            results = pool.map(worker, site_infos)

        failed_count = 0
        for site_id, site_name, message, success in results:
            site_runtime = (task_state.get("sites") or {}).get(site_id) or self._build_site_runtime(site_map.get(site_id))
            site_runtime["site_name"] = site_name
            site_runtime["attempts"] = int(site_runtime.get("attempts") or 0) + 1
            site_runtime["last_message"] = message
            site_runtime["last_run_at"] = self._to_iso(now)
            if success:
                site_runtime["success"] = True
                site_runtime["next_retry_at"] = None
                logger.info(f"站点{task_state.get('label')}成功：{site_name}，尝试次数 {site_runtime['attempts']}")
            else:
                failed_count += 1
                site_runtime["success"] = False
                site_runtime["next_retry_at"] = self._to_iso(self._next_retry_time(now))
                logger.warn(f"站点{task_state.get('label')}失败：{site_name}，"
                            f"下次重试时间 {self._format_clock(self._from_iso(site_runtime['next_retry_at']))}，"
                            f"原因：{message}")
            task_state.setdefault("sites", {})[site_id] = site_runtime

            if self._need_refresh_cookie(message) and getattr(self, "eventmanager", None):
                logger.info(f"触发站点 {site_name} 自动登录更新 Cookie 和 UA")
                self.eventmanager.send_event(EventType.PluginAction,
                                             {
                                                 "site_id": site_id,
                                                 "action": "site_refresh"
                                             })

        if self._auto_cf and int(self._auto_cf or 0) > 0 and failed_count >= int(self._auto_cf or 0):
            if getattr(self, "eventmanager", None):
                self.eventmanager.send_event(EventType.PluginAction, {
                    "action": "cloudflare_speedtest"
                })

        self._append_daily_log(now, results)
        self._save_legacy_task_history(task_key, state, now)
        unresolved_after = len(self._get_pending_site_ids(state, task_key))
        logger.info(f"站点{task_state.get('label')}任务完成，剩余未完成站点 {unresolved_after}")
        self._maybe_send_batch_notification(task_key=task_key,
                                            reason=reason,
                                            task_state=task_state,
                                            run_site_ids=normalized_site_ids,
                                            results=results,
                                            unresolved_before=unresolved_before,
                                            unresolved_after=unresolved_after)
        return results

    def _run_final_check(self, state: dict, now: datetime):
        attempted = False
        logger.info(f"开始执行站点自动签到最终检查，计划时间 {self._final_check_time}")
        for task_key in ("signin", "login"):
            pending_site_ids = self._get_pending_site_ids(state, task_key)
            if pending_site_ids:
                attempted = True
                self._run_task(task_key=task_key,
                               site_ids=pending_site_ids,
                               state=state,
                               now=now,
                               reason="final_check")

        state["final_check_done"] = True
        failed_lines = []
        for task_key in ("signin", "login"):
            task_state = (state.get("tasks") or {}).get(task_key) or {}
            for site_id in self._get_pending_site_ids(state, task_key):
                site_runtime = (task_state.get("sites") or {}).get(site_id) or {}
                failed_lines.append(f"【{task_state.get('label')}】{site_runtime.get('site_name')}：{site_runtime.get('last_message')}")

        if failed_lines:
            failed_text = "\n".join(failed_lines)
            logger.warn(f"站点自动签到最终检查后仍有未完成站点：{failed_text}")
            self.post_message(title="【站点自动签到最终检查】",
                              mtype=NotificationType.SiteMessage,
                              text=f"最终检查时间: {self._final_check_time}\n"
                                   f"以下站点在最终检查后仍未完成，插件会继续按 15-20 分钟随机重试：\n"
                                   f"{failed_text}")
        elif attempted and self._notify:
            logger.info("站点自动签到最终检查完成，所有站点均已完成")
            self.post_message(title="【站点自动签到最终检查】",
                              mtype=NotificationType.SiteMessage,
                              text=f"最终检查时间: {self._final_check_time}\n所有已配置站点的签到和模拟登录均已完成。")

    def __build_class(self, url) -> Any:
        for site_schema in self._site_schema:
            try:
                if site_schema.match(url):
                    return site_schema
            except Exception as e:
                logger.error("站点模块加载失败：%s" % str(e))
        return None

    def signin_by_domain(self, url: str, apikey: str) -> schemas.Response:
        """
        签到一个站点，可由API调用
        """
        # 校验
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        domain = StringUtils.get_url_domain(url)
        site_info = self.sites.get_indexer(domain)
        if not site_info:
            return schemas.Response(
                success=True,
                message=f"站点【{url}】不存在"
            )
        else:
            return schemas.Response(
                success=True,
                message=self.signin_site(site_info)
            )

    def signin_site(self, site_info: CommentedMap) -> Tuple[str, str]:
        """
        签到一个站点
        """
        site_module = self.__build_class(site_info.get("url"))
        # 开始记时
        start_time = datetime.now()
        if site_module and hasattr(site_module, "signin"):
            try:
                state, message = site_module().signin(site_info)
            except Exception as e:
                traceback.print_exc()
                state, message = False, f"签到失败：{str(e)}"
        else:
            state, message = self.__signin_base(site_info)
        # 统计
        seconds = (datetime.now() - start_time).seconds
        domain = StringUtils.get_url_domain(site_info.get('url'))
        if state:
            self.sitestatistic.success(domain=domain, seconds=seconds)
        else:
            self.sitestatistic.fail(domain)
        return site_info.get("name"), message

    @staticmethod
    def __signin_base(site_info: CommentedMap) -> Tuple[bool, str]:
        """
        通用签到处理
        :param site_info: 站点信息
        :return: 签到结果信息
        """
        if not site_info:
            return False, ""
        site = site_info.get("name")
        site_url = site_info.get("url")
        site_cookie = site_info.get("cookie")
        ua = site_info.get("ua")
        render = site_info.get("render")
        proxies = settings.PROXY if site_info.get("proxy") else None
        proxy_server = settings.PROXY_SERVER if site_info.get("proxy") else None
        if not site_url or not site_cookie:
            logger.warn(f"未配置 {site} 的站点地址或Cookie，无法签到")
            return False, ""
        # 模拟登录
        try:
            # 访问链接
            checkin_url = site_url
            if site_url.find("attendance.php") == -1:
                # 拼登签到地址
                checkin_url = urljoin(site_url, "attendance.php")
            logger.info(f"开始站点签到：{site}，地址：{checkin_url}...")
            if render:
                page_source = PlaywrightHelper().get_page_source(url=checkin_url,
                                                                 cookies=site_cookie,
                                                                 ua=ua,
                                                                 proxies=proxy_server)
                if not SiteUtils.is_logged_in(page_source):
                    if under_challenge(page_source):
                        return False, f"无法通过Cloudflare！"
                    return False, f"仿真登录失败，Cookie已失效！"
                else:
                    # 判断是否已签到
                    if re.search(r'已签|签到已得', page_source, re.IGNORECASE) \
                            or SiteUtils.is_checkin(page_source):
                        return True, f"签到成功"
                    return True, "仿真签到成功"
            else:
                res = RequestUtils(cookies=site_cookie,
                                   ua=ua,
                                   proxies=proxies
                                   ).get_res(url=checkin_url)
                if not res and site_url != checkin_url:
                    logger.info(f"开始站点模拟登录：{site}，地址：{site_url}...")
                    res = RequestUtils(cookies=site_cookie,
                                       ua=ua,
                                       proxies=proxies
                                       ).get_res(url=site_url)
                # 判断登录状态
                if res and res.status_code in [200, 500, 403]:
                    if not SiteUtils.is_logged_in(res.text):
                        if under_challenge(res.text):
                            msg = "站点被Cloudflare防护，请打开站点浏览器仿真"
                        elif res.status_code == 200:
                            msg = "Cookie已失效"
                        else:
                            msg = f"状态码：{res.status_code}"
                        logger.warn(f"{site} 签到失败，{msg}")
                        return False, f"签到失败，{msg}！"
                    else:
                        logger.info(f"{site} 签到成功")
                        return True, f"签到成功"
                elif res is not None:
                    logger.warn(f"{site} 签到失败，状态码：{res.status_code}")
                    return False, f"签到失败，状态码：{res.status_code}！"
                else:
                    logger.warn(f"{site} 签到失败，无法打开网站")
                    return False, f"签到失败，无法打开网站！"
        except Exception as e:
            logger.warn("%s 签到失败：%s" % (site, str(e)))
            traceback.print_exc()
            return False, f"签到失败：{str(e)}！"

    def login_site(self, site_info: CommentedMap) -> Tuple[str, str]:
        """
        模拟登录一个站点
        """
        site_module = self.__build_class(site_info.get("url"))
        # 开始记时
        start_time = datetime.now()
        if site_module and hasattr(site_module, "login"):
            try:
                state, message = site_module().login(site_info)
            except Exception as e:
                traceback.print_exc()
                state, message = False, f"模拟登录失败：{str(e)}"
        else:
            state, message = self.__login_base(site_info)
        # 统计
        seconds = (datetime.now() - start_time).seconds
        domain = StringUtils.get_url_domain(site_info.get('url'))
        if state:
            self.sitestatistic.success(domain=domain, seconds=seconds)
        else:
            self.sitestatistic.fail(domain)
        return site_info.get("name"), message

    @staticmethod
    def __login_base(site_info: CommentedMap) -> Tuple[bool, str]:
        """
        模拟登录通用处理
        :param site_info: 站点信息
        :return: 签到结果信息
        """
        if not site_info:
            return False, ""
        site = site_info.get("name")
        site_url = site_info.get("url")
        site_cookie = site_info.get("cookie")
        ua = site_info.get("ua")
        render = site_info.get("render")
        proxies = settings.PROXY if site_info.get("proxy") else None
        proxy_server = settings.PROXY_SERVER if site_info.get("proxy") else None
        if not site_url or not site_cookie:
            logger.warn(f"未配置 {site} 的站点地址或Cookie，无法签到")
            return False, ""
        # 模拟登录
        try:
            # 访问链接
            site_url = str(site_url).replace("attendance.php", "")
            logger.info(f"开始站点模拟登录：{site}，地址：{site_url}...")
            if render:
                page_source = PlaywrightHelper().get_page_source(url=site_url,
                                                                 cookies=site_cookie,
                                                                 ua=ua,
                                                                 proxies=proxy_server)
                if not SiteUtils.is_logged_in(page_source):
                    if under_challenge(page_source):
                        return False, f"无法通过Cloudflare！"
                    return False, f"仿真登录失败，Cookie已失效！"
                else:
                    return True, "模拟登录成功"
            else:
                res = RequestUtils(cookies=site_cookie,
                                   ua=ua,
                                   proxies=proxies
                                   ).get_res(url=site_url)
                # 判断登录状态
                if res and res.status_code in [200, 500, 403]:
                    if not SiteUtils.is_logged_in(res.text):
                        if under_challenge(res.text):
                            msg = "站点被Cloudflare防护，请打开站点浏览器仿真"
                        elif res.status_code == 200:
                            msg = "Cookie已失效"
                        else:
                            msg = f"状态码：{res.status_code}"
                        logger.warn(f"{site} 模拟登录失败，{msg}")
                        return False, f"模拟登录失败，{msg}！"
                    else:
                        logger.info(f"{site} 模拟登录成功")
                        return True, f"模拟登录成功"
                elif res is not None:
                    logger.warn(f"{site} 模拟登录失败，状态码：{res.status_code}")
                    return False, f"模拟登录失败，状态码：{res.status_code}！"
                else:
                    logger.warn(f"{site} 模拟登录失败，无法打开网站")
                    return False, f"模拟登录失败，无法打开网站！"
        except Exception as e:
            logger.warn("%s 模拟登录失败：%s" % (site, str(e)))
            traceback.print_exc()
            return False, f"模拟登录失败：{str(e)}！"

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error("退出插件失败：%s" % str(e))

    @eventmanager.register(EventType.SiteDeleted)
    def site_deleted(self, event):
        """
        删除对应站点选中
        """
        site_id = event.event_data.get("site_id")
        config = self.get_config()
        if config:
            self._sign_sites = self.__remove_site_id(config.get("sign_sites") or [], site_id)
            self._login_sites = self.__remove_site_id(config.get("login_sites") or [], site_id)
            # 保存配置
            self.__update_config()

    def __remove_site_id(self, do_sites, site_id):
        if do_sites:
            if isinstance(do_sites, str):
                do_sites = [do_sites]

            # 删除对应站点
            if site_id:
                do_sites = [site for site in do_sites if int(site) != int(site_id)]
            else:
                # 清空
                do_sites = []

            # 若无站点，则停止
            if len(do_sites) == 0:
                self._enabled = False

        return do_sites

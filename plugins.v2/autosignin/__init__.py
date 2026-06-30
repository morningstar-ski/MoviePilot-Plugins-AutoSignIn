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
from app import schemas
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.db.site_oper import SiteOper
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
from apscheduler.schedulers.background import BackgroundScheduler
from ruamel.yaml import CommentedMap

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
    plugin_author = "morningstar-ski"
    # 作者主页
    author_url = "https://github.com/morningstar-ski/MoviePilot-Plugins-Custom"
    # 插件配置项ID前缀
    plugin_config_prefix = "autosignin_"
    # 加载顺序
    plugin_order = 0
    # 可使用的用户级别
    auth_level = 2

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
            all_sites = [site.id for site in SiteOper().list_order_by_pri()] + [site.get("id") for site in
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
                logger.info("AutoSignIn startup reconcile will run in 15 seconds")
                self._scheduler.add_job(func=self.service_tick,
                                        trigger='date',
                                        run_date=self._now() + timedelta(seconds=self._startup_reconcile_delay_seconds),
                                        id="AutoSignInStartupTick",
                                        name="AutoSignIn startup reconcile",
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
                         for site in SiteOper().list_order_by_pri()]
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
        # 获取最近14天的日期数组
        date_list = [(datetime.now() - timedelta(days=i)).date() for i in range(14)]

        # 获取所有数据，包括签到和登录历史
        all_data = {
            "signin": [],  # 签到数据
            "login": []  # 登录数据
        }
        sites_info = self._build_sites_info()

        # 获取常规日期格式数据
        for day in date_list:
            day_str = f"{day.month}月{day.day}日"
            day_formatted = day.strftime('%Y-%m-%d')

            # 获取"月日"格式数据
            day_data = self.get_data(day_str)
            if day_data:
                # 添加日期信息到每条记录
                if isinstance(day_data, list):
                    for record in day_data:
                        if isinstance(record, dict):
                            record["date"] = day_str
                            record["day_obj"] = day
                            # 区分签到和登录数据
                            if "登录" in record.get("status", ""):
                                all_data["login"].append(record)
                            else:
                                all_data["signin"].append(record)

            # 获取"签到-yyyy-mm-dd"和"登录-yyyy-mm-dd"格式数据
            signin_history = self.get_data(key="签到-" + day_formatted)
            if signin_history and isinstance(signin_history, dict):
                # 获取完成签到的站点ID列表
                done_sites = signin_history.get("do", [])
                retry_sites = signin_history.get("retry", [])

                # 为所有已完成签到的站点创建记录
                for site_id in done_sites:
                    site_name = self._get_site_display_name(site_id=site_id, sites_info=sites_info)
                    if not site_name:
                        continue

                    # 跳过需要重试的站点
                    if site_id in retry_sites:
                        status_text = "需要重试"
                    else:
                        status_text = "已签到"
                    all_data["signin"].append({
                        "site": site_name,
                        "status": status_text,
                        "date": day_str,
                        "day_obj": day,
                        "site_id": site_id
                    })

            # 获取登录历史数据
            login_history = self.get_data(key="登录-" + day_formatted)
            if login_history and isinstance(login_history, dict):
                # 获取完成登录的站点ID列表
                done_sites = login_history.get("do", [])
                retry_sites = login_history.get("retry", [])

                # 为所有已完成登录的站点创建记录
                for site_id in done_sites:
                    site_name = self._get_site_display_name(site_id=site_id, sites_info=sites_info)
                    if not site_name:
                        continue

                    # 跳过需要重试的站点
                    if site_id in retry_sites:
                        status_text = "登录需要重试"
                    else:
                        status_text = "登录成功"
                    all_data["login"].append({
                        "site": site_name,
                        "status": status_text,
                        "date": day_str,
                        "day_obj": day,
                        "site_id": site_id
                    })

        # 如果没有数据且没有配置站点，显示提示信息
        if not all_data["signin"] and not all_data["login"] and not self._sign_sites and not self._login_sites:
            return [{
                'component': 'VAlert',
                'props': {
                    'type': 'info',
                    'text': '暂无签到数据',
                    'variant': 'tonal',
                    'class': 'mt-4',
                    'prepend-icon': 'mdi-information'
                }
            }]

        # 按站点分组并去重数据
        signin_site_data = {}
        login_site_data = {}

        # 处理签到数据 - 每个站点每天只保留一条最新记录
        site_day_records = {}  # 用于去重: {site}_{date} -> record
        for data in all_data["signin"]:
            site_name = data.get("site", "未知站点")
            date_str = data.get("date", "")
            site_day_key = f"{site_name}_{date_str}"

            # 存储或更新记录（如有多条取最新）
            site_day_records[site_day_key] = data

        # 整理去重后的数据
        for key, record in site_day_records.items():
            site_name = record.get("site", "未知站点")
            if site_name not in signin_site_data:
                signin_site_data[site_name] = []
            signin_site_data[site_name].append(record)

        # 处理登录数据 - 同样去重
        site_day_records = {}  # 重置去重字典
        for data in all_data["login"]:
            site_name = data.get("site", "未知站点")
            date_str = data.get("date", "")
            site_day_key = f"{site_name}_{date_str}"

            # 存储或更新记录
            site_day_records[site_day_key] = data

        # 整理去重后的数据
        for key, record in site_day_records.items():
            site_name = record.get("site", "未知站点")
            if site_name not in login_site_data:
                login_site_data[site_name] = []
            login_site_data[site_name].append(record)

        # 补齐已配置但暂无历史记录的站点，详情页能直接看出未记录项。
        for site_id in self._sign_sites:
            site_name = self._get_site_display_name(site_id=site_id, sites_info=sites_info)
            if not site_name:
                continue
            signin_site_data.setdefault(site_name, [])
        for site_id in self._login_sites:
            site_name = self._get_site_display_name(site_id=site_id, sites_info=sites_info)
            if not site_name:
                continue
            login_site_data.setdefault(site_name, [])

        display_dates = date_list[:7]
        today_label = self._date_label(day=date_list[0])
        signin_stats = self._calculate_day_stats(site_data=signin_site_data, date_label=today_label)
        login_stats = self._calculate_day_stats(site_data=login_site_data, date_label=today_label)

        # 添加紧凑状态矩阵样式
        return [
            {
                'component': 'style',
                'text': """
                .autosignin-page {
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                }
                .autosignin-summary {
                    display: grid;
                    grid-template-columns: repeat(4, minmax(0, 1fr));
                    gap: 8px;
                }
                .autosignin-stat {
                    min-width: 0;
                    padding: 10px 12px;
                }
                .autosignin-stat__head {
                    display: flex;
                    align-items: center;
                    gap: 6px;
                    min-width: 0;
                    color: rgba(var(--v-theme-on-surface), var(--v-medium-emphasis-opacity));
                    font-size: .75rem;
                    font-weight: 600;
                    line-height: 1.25;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }
                .autosignin-stat__head .v-icon {
                    color: rgb(var(--app-card-accent-rgb));
                }
                .autosignin-stat__value {
                    margin-top: 8px;
                    font-size: 1.25rem;
                    font-weight: 700;
                    line-height: 1;
                    letter-spacing: 0;
                }
                .autosignin-stat__meta {
                    margin-top: 4px;
                    color: rgba(var(--v-theme-on-surface), .56);
                    font-size: .72rem;
                    line-height: 1.25;
                    white-space: nowrap;
                    overflow: hidden;
                    text-overflow: ellipsis;
                }
                .autosignin-section {
                    min-width: 0;
                }
                .autosignin-section-head {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    min-height: 30px;
                    margin-bottom: 6px;
                }
                .autosignin-section-title {
                    font-size: .95rem;
                    font-weight: 700;
                    letter-spacing: 0;
                }
                .autosignin-table-wrap {
                    overflow-x: auto;
                    border: 1px solid rgba(var(--v-theme-on-surface), .08);
                    border-radius: 8px;
                }
                .autosignin-table {
                    min-width: 620px;
                }
                html[data-theme="transparent"] .autosignin-table-wrap,
                .v-theme--transparent .autosignin-table-wrap {
                    backdrop-filter: blur(var(--transparent-blur, 10px));
                    background-color: rgba(var(--v-theme-surface), 0) !important;
                }
                html[data-theme="transparent"] .autosignin-table,
                html[data-theme="transparent"] .autosignin-table .v-table__wrapper,
                html[data-theme="transparent"] .autosignin-table table,
                html[data-theme="transparent"] .autosignin-table tbody tr,
                .v-theme--transparent .autosignin-table,
                .v-theme--transparent .autosignin-table .v-table__wrapper,
                .v-theme--transparent .autosignin-table table,
                .v-theme--transparent .autosignin-table tbody tr {
                    background-color: transparent !important;
                }
                .autosignin-table th {
                    height: 34px !important;
                    padding: 0 8px !important;
                    color: rgba(var(--v-theme-on-surface), .62);
                    font-size: .75rem;
                    font-weight: 600 !important;
                    white-space: nowrap;
                }
                .autosignin-table td {
                    height: 38px !important;
                    padding: 0 8px !important;
                    vertical-align: middle;
                }
                .autosignin-table tbody tr:last-child td {
                    border-bottom: 0 !important;
                }
                .autosignin-site-name {
                    max-width: 160px;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                    font-weight: 600;
                    line-height: 1.2;
                }
                .autosignin-site-meta {
                    margin-top: 2px;
                    color: rgba(var(--v-theme-on-surface), .52);
                    font-size: .68rem;
                    line-height: 1.1;
                }
                .autosignin-status-cell {
                    min-width: 92px;
                }
                .autosignin-dot-cell {
                    width: 40px;
                    text-align: center;
                }
                .autosignin-dot {
                    width: 22px;
                    height: 22px;
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    border-radius: 999px;
                    border: 1px solid transparent;
                    font-weight: 700;
                }
                .autosignin-dot .v-icon {
                    opacity: 1;
                }
                .autosignin-dot--success {
                    color: rgb(var(--v-theme-success));
                    background: rgba(var(--v-theme-success), .24);
                    border-color: rgba(var(--v-theme-success), .38);
                }
                .autosignin-dot--warning {
                    color: rgb(var(--v-theme-warning));
                    background: rgba(var(--v-theme-warning), .30);
                    border-color: rgba(var(--v-theme-warning), .48);
                }
                .autosignin-dot--error {
                    color: rgb(var(--v-theme-error));
                    background: rgba(var(--v-theme-error), .26);
                    border-color: rgba(var(--v-theme-error), .42);
                }
                .autosignin-dot--none {
                    color: rgba(var(--v-theme-on-surface), .68);
                    background: rgba(var(--v-theme-on-surface), .14);
                    border-color: rgba(var(--v-theme-on-surface), .22);
                }
                @media (max-width: 720px) {
                    .autosignin-page {
                        gap: 10px;
                    }
                    .autosignin-summary {
                        grid-template-columns: repeat(2, minmax(0, 1fr));
                    }
                    .autosignin-stat {
                        padding: 8px 10px;
                    }
                    .autosignin-stat__value {
                        font-size: 1.08rem;
                    }
                    .autosignin-table {
                        min-width: 560px;
                    }
                    .autosignin-table th,
                    .autosignin-table td {
                        padding-left: 6px !important;
                        padding-right: 6px !important;
                    }
                    .autosignin-site-name {
                        max-width: 126px;
                    }
                    .autosignin-dot-cell {
                        width: 34px;
                    }
                }
                """
            },
            {
                'component': 'div',
                'props': {
                    'class': 'autosignin-page'
                },
                'content': [
                    self._build_summary(signin_stats=signin_stats, login_stats=login_stats, days=len(date_list)),
                    self._build_status_section(
                        title="签到状态",
                        icon="mdi-calendar-check",
                        site_data=signin_site_data,
                        display_dates=display_dates,
                        empty_text="暂无签到数据"
                    ),
                    self._build_status_section(
                        title="登录状态",
                        icon="mdi-login-variant",
                        site_data=login_site_data,
                        display_dates=display_dates,
                        empty_text="暂无登录数据"
                    )
                ]
            }
        ]

    @staticmethod
    def _add_site_info(sites_info: dict, site_id: Any, site_name: Any) -> None:
        """
        记录站点ID到名称的映射，兼容历史记录中ID类型不一致的情况。
        """
        if site_id is None or not site_name:
            return
        sites_info[site_id] = site_name
        sites_info[str(site_id)] = site_name

    def _build_sites_info(self) -> dict:
        """
        汇总系统站点、索引器站点和自定义站点名称，供详情页历史记录反查。
        """
        sites_info = {}
        for site in SitesHelper().get_indexers():
            if not site.get("public"):
                self._add_site_info(
                    sites_info=sites_info,
                    site_id=site.get("id"),
                    site_name=site.get("name")
                )
        for site in SiteOper().list_order_by_pri():
            self._add_site_info(
                sites_info=sites_info,
                site_id=getattr(site, "id", None),
                site_name=getattr(site, "name", None)
            )
        for site in self.__custom_sites():
            self._add_site_info(
                sites_info=sites_info,
                site_id=site.get("id"),
                site_name=site.get("name")
            )
        return sites_info

    @staticmethod
    def _get_site_display_name(site_id, sites_info: dict) -> Optional[str]:
        """
        根据站点ID获取详情页中展示的站点名称，查不到时返回空值便于跳过。
        """
        site_id_str = str(site_id)
        return sites_info.get(site_id_str) or sites_info.get(site_id)

    @staticmethod
    def _status_meta(status_text: str) -> dict:
        """
        将签到或登录状态文本转换为页面展示需要的颜色、图标和排序权重。
        """
        status_text = str(status_text or "").strip()
        if "Cookie已失效" in status_text or "失效" in status_text:
            return {
                "level": "error",
                "color": "error",
                "icon": "mdi-cookie-off",
                "label": status_text or "Cookie失效",
                "sort": 0
            }
        if "失败" in status_text or "错误" in status_text:
            return {
                "level": "error",
                "color": "error",
                "icon": "mdi-alert-circle",
                "label": status_text or "失败",
                "sort": 0
            }
        if "重试" in status_text:
            return {
                "level": "warning",
                "color": "warning",
                "icon": "mdi-refresh",
                "label": status_text or "需要重试",
                "sort": 1
            }
        if "成功" in status_text or "已签到" in status_text:
            return {
                "level": "success",
                "color": "success",
                "icon": "mdi-check-circle",
                "label": status_text or "成功",
                "sort": 3
            }
        if status_text:
            return {
                "level": "none",
                "color": "grey",
                "icon": "mdi-help-circle-outline",
                "label": status_text,
                "sort": 2
            }
        return {
            "level": "none",
            "color": "grey",
            "icon": "mdi-minus-circle-outline",
            "label": "未记录",
            "sort": 2
        }

    @staticmethod
    def _latest_record(records: list, date_label: str = None) -> dict:
        """
        获取指定日期或整个记录列表中的最新一条记录。
        """
        filtered_records = records
        if date_label:
            filtered_records = [record for record in records if record.get("date") == date_label]
        if not filtered_records:
            return {}
        try:
            return sorted(
                filtered_records,
                key=lambda item: item.get("day_obj", datetime.min.date()),
                reverse=True
            )[0]
        except Exception as e:
            logger.debug(f"获取最新记录失败: {str(e)}")
            return filtered_records[0]

    @staticmethod
    def _date_label(day) -> str:
        """
        将日期对象格式化为历史记录使用的月日标签。
        """
        return f"{day.month}月{day.day}日"

    @classmethod
    def _calculate_day_stats(cls, site_data: dict, date_label: str) -> dict:
        """
        统计指定日期下各站点的成功、异常和未记录数量。
        """
        stats = {
            "total": len(site_data),
            "recorded": 0,
            "success": 0,
            "warning": 0,
            "error": 0,
            "none": 0
        }
        for records in site_data.values():
            record = cls._latest_record(records=records, date_label=date_label)
            if not record:
                stats["none"] += 1
                continue
            stats["recorded"] += 1
            level = cls._status_meta(record.get("status", "")).get("level")
            if level in stats:
                stats[level] += 1
        return stats

    @staticmethod
    def _build_stat_item(label: str, value: str, meta: str, color: str, icon: str) -> dict:
        """
        构建顶部紧凑统计块。
        """
        return {
            'component': 'div',
            'props': {
                'class': 'autosignin-stat app-card-shell app-card-colorful',
                'style': f'--app-card-accent-rgb: var(--v-theme-{color});'
            },
            'content': [
                {
                    'component': 'div',
                    'props': {
                        'class': 'autosignin-stat__head'
                    },
                    'content': [
                        {
                            'component': 'VIcon',
                            'props': {
                                'size': 'x-small',
                                'color': color
                            },
                            'text': icon
                        },
                        {
                            'component': 'span',
                            'text': label
                        }
                    ]
                },
                {
                    'component': 'div',
                    'props': {
                        'class': f'autosignin-stat__value text-{color}'
                    },
                    'text': value
                },
                {
                    'component': 'div',
                    'props': {
                        'class': 'autosignin-stat__meta'
                    },
                    'text': meta
                }
            ]
        }

    @classmethod
    def _build_summary(cls, signin_stats: dict, login_stats: dict, days: int) -> dict:
        """
        构建详情页顶部的签到和登录概要。
        """
        signin_total = signin_stats.get("total") or 0
        login_total = login_stats.get("total") or 0
        signin_problem_count = (signin_stats.get("error") or 0) + (signin_stats.get("warning") or 0)
        signin_missing_count = signin_stats.get("none") or 0
        signin_color = "info"
        if signin_total:
            signin_color = "success" if not signin_problem_count and not signin_missing_count else "warning"
        if signin_total and signin_stats.get("error"):
            signin_color = "error"

        return {
            'component': 'div',
            'props': {
                'class': 'autosignin-summary'
            },
            'content': [
                cls._build_stat_item(
                    label="今日签到",
                    value=f"{signin_stats.get('success') or 0}/{signin_total}",
                    meta=f"异常 {signin_problem_count} · 未记录 {signin_missing_count}",
                    color=signin_color,
                    icon="mdi-calendar-check"
                ),
                cls._build_stat_item(
                    label="异常重试",
                    value=str(signin_problem_count),
                    meta=f"失败 {signin_stats.get('error') or 0} · 重试 {signin_stats.get('warning') or 0}",
                    color="error" if signin_stats.get("error") else "warning",
                    icon="mdi-alert-circle-outline"
                ),
                cls._build_stat_item(
                    label="今日登录",
                    value=f"{login_stats.get('success') or 0}/{login_total}",
                    meta=f"异常 {(login_stats.get('error') or 0) + (login_stats.get('warning') or 0)} · 未记录 {login_stats.get('none') or 0}",
                    color="success" if login_total and not login_stats.get("error") and not login_stats.get("warning") else "info",
                    icon="mdi-login-variant"
                ),
                cls._build_stat_item(
                    label="历史范围",
                    value=f"{days}天",
                    meta="矩阵显示最近7天",
                    color="info",
                    icon="mdi-history"
                )
            ]
        }

    @classmethod
    def _build_status_section(cls, title: str, icon: str, site_data: dict, display_dates: list, empty_text: str) -> dict:
        """
        构建签到或登录状态区块，使用按站点排列的紧凑矩阵展示最近状态。
        """
        return {
            'component': 'div',
            'props': {
                'class': 'autosignin-section'
            },
            'content': [
                {
                    'component': 'div',
                    'props': {
                        'class': 'autosignin-section-head'
                    },
                    'content': [
                        {
                            'component': 'VIcon',
                            'props': {
                                'size': 'small',
                                'color': 'primary'
                            },
                            'text': icon
                        },
                        {
                            'component': 'span',
                            'props': {
                                'class': 'autosignin-section-title'
                            },
                            'text': title
                        },
                        {
                            'component': 'VSpacer'
                        },
                        {
                            'component': 'VChip',
                            'props': {
                                'size': 'x-small',
                                'variant': 'tonal',
                                'color': 'primary'
                            },
                            'text': f"{len(site_data)} 个站点"
                        }
                    ]
                },
                cls._build_status_table(
                    site_data=site_data,
                    display_dates=display_dates,
                    empty_text=empty_text
                )
            ]
        }

    @classmethod
    def _build_status_table(cls, site_data: dict, display_dates: list, empty_text: str) -> dict:
        """
        构建按站点和日期交叉展示的状态表格。
        """
        if not site_data:
            return {
                'component': 'VAlert',
                'props': {
                    'type': 'info',
                    'text': empty_text,
                    'variant': 'tonal',
                    'density': 'compact',
                    'prepend-icon': 'mdi-information'
                }
            }

        table_headers = [
            {
                'component': 'th',
                'props': {
                    'class': 'text-start'
                },
                'text': '站点'
            },
            {
                'component': 'th',
                'props': {
                    'class': 'text-start'
                },
                'text': '今日'
            }
        ]
        for day in display_dates:
            table_headers.append({
                'component': 'th',
                'props': {
                    'class': 'text-center'
                },
                'text': f"{day.month}/{day.day}"
            })

        sorted_sites = sorted(
            site_data.items(),
            key=lambda item: cls._site_sort_key(site_name=item[0], records=item[1], display_dates=display_dates)
        )
        table_rows = []
        for site_name, records in sorted_sites:
            table_rows.append(cls._build_status_row(site_name=site_name, records=records, display_dates=display_dates))

        return {
            'component': 'div',
            'props': {
                'class': 'autosignin-table-wrap'
            },
            'content': [
                {
                    'component': 'VTable',
                    'props': {
                        'hover': True,
                        'density': 'compact',
                        'class': 'autosignin-table'
                    },
                    'content': [
                        {
                            'component': 'thead',
                            'content': [
                                {
                                    'component': 'tr',
                                    'content': table_headers
                                }
                            ]
                        },
                        {
                            'component': 'tbody',
                            'content': table_rows
                        }
                    ]
                }
            ]
        }

    @classmethod
    def _site_sort_key(cls, site_name: str, records: list, display_dates: list) -> tuple:
        """
        生成站点行排序键，让今日异常和未记录站点优先展示。
        """
        today_label = cls._date_label(day=display_dates[0]) if display_dates else ""
        today_record = cls._latest_record(records=records, date_label=today_label)
        latest_record = cls._latest_record(records=records)
        status_meta = cls._status_meta(today_record.get("status", "") if today_record else "")
        latest_day = latest_record.get("day_obj", datetime.min.date()) if latest_record else datetime.min.date()
        return status_meta.get("sort", 2), -latest_day.toordinal(), site_name

    @classmethod
    def _build_status_row(cls, site_name: str, records: list, display_dates: list) -> dict:
        """
        构建单个站点在状态矩阵中的一行。
        """
        today_label = cls._date_label(day=display_dates[0]) if display_dates else ""
        today_record = cls._latest_record(records=records, date_label=today_label)
        today_status = today_record.get("status", "") if today_record else ""
        today_meta = cls._status_meta(today_status)
        row_cells = [
            {
                'component': 'td',
                'content': [
                    {
                        'component': 'div',
                        'props': {
                            'class': 'autosignin-site-name',
                            'title': site_name
                        },
                        'text': site_name
                    },
                    {
                        'component': 'div',
                        'props': {
                            'class': 'autosignin-site-meta'
                        },
                        'text': f"{len(records)} 条记录" if records else "暂无记录"
                    }
                ]
            },
            {
                'component': 'td',
                'props': {
                    'class': 'autosignin-status-cell'
                },
                'content': [
                    {
                        'component': 'VChip',
                        'props': {
                            'size': 'x-small',
                            'variant': 'tonal',
                            'color': today_meta.get("color"),
                            'prepend-icon': today_meta.get("icon")
                        },
                        'text': today_meta.get("label")
                    }
                ]
            }
        ]
        for day in display_dates:
            date_label = cls._date_label(day=day)
            record = cls._latest_record(records=records, date_label=date_label)
            row_cells.append({
                'component': 'td',
                'props': {
                    'class': 'autosignin-dot-cell'
                },
                'content': [
                    cls._build_status_dot(record=record, date_label=date_label)
                ]
            })
        return {
            'component': 'tr',
            'content': row_cells
        }

    @classmethod
    def _build_status_dot(cls, record: dict, date_label: str) -> dict:
        """
        构建矩阵中单日状态的图标点。
        """
        status_text = record.get("status", "") if record else ""
        status_meta = cls._status_meta(status_text)
        return {
            'component': 'span',
            'props': {
                'class': f"autosignin-dot autosignin-dot--{status_meta.get('level')}",
                'title': f"{date_label} {status_meta.get('label')}"
            },
            'content': [
                {
                    'component': 'VIcon',
                    'props': {
                        'size': 'x-small'
                    },
                    'text': status_meta.get("icon")
                }
            ]
        }

    @eventmanager.register(EventType.PluginAction)
    def sign_in(self, event: Event = None):
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "site_signin":
                return

        if not self._schedule_lock.acquire(blocking=False):
            logger.warn("AutoSignIn task is already running, skip this request")
            return

        try:
            now = self._now()
            state = self._ensure_schedule_state(now)

            if event:
                logger.info("Receive manual site signin command")
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
            logger.error(f"AutoSignIn manual task failed: {err}")
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
            logger.info("AutoSignIn task is still running, skip this heartbeat")
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
            logger.error(f"AutoSignIn heartbeat failed: {err}")
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
        all_sites = [site for site in SitesHelper().get_indexers() if not site.get("public")] + self.__custom_sites()
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
        return ", ".join(site_names) if site_names else "none"

    def _parse_final_check_time(self, now: datetime) -> datetime:
        try:
            hour_str, minute_str = str(self._final_check_time or "22:00").split(":", 1)
            hour = int(hour_str)
            minute = int(minute_str)
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                raise ValueError("invalid final check time")
        except Exception:
            logger.error(f"Invalid final check time: {self._final_check_time}, fallback to 22:00")
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
        logger.info(f"AutoSignIn daily plan generated: signin={self._format_clock(signin_at)} "
                    f"login={self._format_clock(login_at)} final_check={self._format_clock(final_check_at)}")
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
        logger.debug(f"AutoSignIn heartbeat state: date={state.get('date')} final_check={self._format_clock(self._from_iso(state.get('final_check_at')))}")
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
        logger.info(f"Start AutoSignIn task: {task_state.get('label')} reason={reason} "
                    f"count={len(site_infos)} sites={self._format_site_list(site_map, normalized_site_ids)}")

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
                logger.info(f"AutoSignIn {task_state.get('label')} success: {site_name} attempts={site_runtime['attempts']}")
            else:
                failed_count += 1
                site_runtime["success"] = False
                site_runtime["next_retry_at"] = self._to_iso(self._next_retry_time(now))
                logger.warn(f"AutoSignIn {task_state.get('label')} failed: {site_name} "
                            f"next_retry={self._format_clock(self._from_iso(site_runtime['next_retry_at']))} "
                            f"message={message}")
            task_state.setdefault("sites", {})[site_id] = site_runtime

            if self._need_refresh_cookie(message) and getattr(self, "eventmanager", None):
                logger.info(f"Trigger site refresh for {site_name}")
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
        logger.info(f"AutoSignIn {task_state.get('label')} run finished, unresolved={unresolved_after}")
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
        logger.info(f"Start AutoSignIn final check at {self._final_check_time}")
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
            logger.warn(f"AutoSignIn final check still has pending sites: {failed_text}")
            self.post_message(title="【站点自动签到最终检查】",
                              mtype=NotificationType.SiteMessage,
                              text=f"最终检查时间: {self._final_check_time}\n"
                                   f"以下站点在最终检查后仍未完成，插件会继续按 15-20 分钟随机重试：\n"
                                   f"{failed_text}")
        elif attempted and self._notify:
            logger.info("AutoSignIn final check completed with all sites done")
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
        site_info = SitesHelper().get_indexer(domain)
        if not site_info:
            return schemas.Response(
                success=True,
                message=f"站点【{url}】不存在"
            )
        else:
            site_name, message = self.signin_site(site_info)
            return schemas.Response(
                success=True,
                message=f"站点【{site_name}】{message or '签到成功'}"
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
            SiteOper().success(domain=domain, seconds=seconds)
        else:
            SiteOper().fail(domain)
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
        timeout = site_info.get("timeout") or 60
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
                                                                 proxies=proxy_server,
                                                                 timeout=timeout)
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
                                   proxies=proxies,
                                   timeout=timeout
                                   ).get_res(url=checkin_url)
                if not res and site_url != checkin_url:
                    logger.info(f"开始站点模拟登录：{site}，地址：{site_url}...")
                    res = RequestUtils(cookies=site_cookie,
                                       ua=ua,
                                       proxies=proxies,
                                       timeout=timeout
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
            SiteOper().success(domain=domain, seconds=seconds)
        else:
            SiteOper().fail(domain)
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
        timeout = site_info.get("timeout") or 60
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
                                                                 proxies=proxy_server,
                                                                 timeout=timeout)
                if not SiteUtils.is_logged_in(page_source):
                    if under_challenge(page_source):
                        return False, f"无法通过Cloudflare！"
                    return False, f"仿真登录失败，Cookie已失效！"
                else:
                    return True, "模拟登录成功"
            else:
                res = RequestUtils(cookies=site_cookie,
                                   ua=ua,
                                   proxies=proxies,
                                   timeout=timeout
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


def record_to_row(record):
    """辅助函数：将记录转换为表格行"""
    status = record.get("status", "")

    # 确定状态图标和颜色
    icon = "mdi-check-circle"
    color = "success"

    if "失败" in status or "错误" in status:
        icon = "mdi-alert-circle"
        color = "error"
    elif "Cookie已失效" in status:
        icon = "mdi-cookie-off"
        color = "error"
    elif "已签到" in status:
        icon = "mdi-check"
        color = "grey"
    elif "成功" in status:
        icon = "mdi-check-circle"
        color = "success"

    return {
        'component': 'tr',
        'props': {
            'class': 'text-sm'
        },
        'content': [
            {
                'component': 'td',
                'props': {
                    'class': 'text-start'
                },
                'text': record.get("date", "")
            },
            {
                'component': 'td',
                'props': {
                    'class': 'text-start'
                },
                'text': status
            },
            {
                'component': 'td',
                'props': {
                    'class': 'text-center'
                },
                'content': [
                    {
                        'component': 'VIcon',
                        'props': {
                            'color': color,
                            'size': 'small'
                        },
                        'text': icon
                    }
                ]
            }
        ]
    }

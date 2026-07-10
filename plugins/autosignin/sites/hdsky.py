import json
import time
from typing import Tuple

from app.core.config import settings
from app.log import logger
from app.plugins.autosignin.captcha import CaptchaSolver
from app.plugins.autosignin.sites import _ISiteSigninHandler
from app.utils.http import RequestUtils
from app.utils.string import StringUtils


class HDSky(_ISiteSigninHandler):
    """
    天空ocr签到
    """
    # 匹配的站点Url，每一个实现类都需要设置为自己的站点Url
    site_url = "hdsky.me"

    # 已签到
    _sign_regex = ['已签到']

    @classmethod
    def match(cls, url: str) -> bool:
        """
        根据站点Url判断是否匹配当前站点签到类，大部分情况使用默认实现即可
        :param url: 站点Url
        :return: 是否匹配，如匹配则会调用该类的signin方法
        """
        return True if StringUtils.url_equal(url, cls.site_url) else False

    def signin(self, site_info: dict) -> Tuple[bool, str]:
        """
        执行签到操作
        :param site_info: 站点信息，含有站点Url、站点Cookie、UA等信息
        :return: 签到结果信息
        """
        site = site_info.get("name")
        site_cookie = site_info.get("cookie")
        ua = site_info.get("ua")
        proxy = site_info.get("proxy")
        render = site_info.get("render")
        referer = site_info.get("url")

        # 判断今日是否已签到
        html_text = self.get_page_source(url='https://hdsky.me',
                                         cookie=site_cookie,
                                         ua=ua,
                                         proxy=proxy,
                                         render=render)
        if not html_text:
            logger.error(f"{site} 签到失败，请检查站点连通性")
            return False, '签到失败，请检查站点连通性'

        if "login.php" in html_text:
            logger.error(f"{site} 签到失败，Cookie已失效")
            return False, '签到失败，Cookie已失效'

        sign_status = self.sign_in_result(html_res=html_text,
                                          regexs=self._sign_regex)
        if sign_status:
            logger.info(f"{site} 今日已签到")
            return True, '今日已签到'

        def _fetch_captcha():
            res_times = 0
            while res_times <= 3:
                image_res = RequestUtils(cookies=site_cookie,
                                         ua=ua,
                                         content_type='application/x-www-form-urlencoded; charset=UTF-8',
                                         referer="https://hdsky.me/index.php",
                                         accept_type="*/*",
                                         proxies=settings.PROXY if proxy else None
                                         ).post_res(url='https://hdsky.me/image_code_ajax.php',
                                                    data={'action': 'new'})
                if image_res and image_res.status_code == 200:
                    try:
                        image_json = json.loads(image_res.text)
                    except Exception as err:
                        logger.warn(f"{site} 获取验证码响应解析失败，正在重试：{err}")
                        image_json = {}
                    if image_json.get("success"):
                        img_hash = image_json.get("code")
                        if img_hash:
                            img_get_url = 'https://hdsky.me/image.php?action=regimage&imagehash=%s' % img_hash
                            logger.info(f"获取到 {site} 验证码链接：{img_get_url}")
                            return img_hash, img_get_url
                res_times += 1
                logger.info(f"获取 {site} 验证码失败，正在进行重试，目前重试次数：{res_times}")
                time.sleep(1)
            return None, None

        last_error = '签到失败：未获取到验证码'
        for _ in range(4):
            img_hash, img_get_url = _fetch_captcha()
            if not img_hash:
                continue

            ocr_result = None
            for times in range(2):
                ocr_result = CaptchaSolver.solve(image_url=img_get_url,
                                                 cookie=site_cookie,
                                                 ua=ua,
                                                 proxy=proxy,
                                                 website_url=referer)
                logger.info(f"OCR识别 {site} 验证码：{ocr_result}")
                if ocr_result and len(ocr_result) == 6:
                    logger.info(f"OCR识别 {site} 验证码成功：{ocr_result}")
                    break
                if times < 1:
                    logger.info(f"OCR识别 {site} 验证码失败，正在进行重试，目前重试次数：{times + 1}")
                    time.sleep(1)

            if not ocr_result or len(ocr_result) != 6:
                last_error = '签到失败：验证码识别失败'
                continue

            data = {
                'action': 'showup',
                'imagehash': img_hash,
                'imagestring': ocr_result
            }
            res = RequestUtils(cookies=site_cookie,
                               ua=ua,
                               referer=referer,
                               proxies=settings.PROXY if proxy else None
                               ).post_res(url='https://hdsky.me/showup.php', data=data)
            if res and res.status_code == 200:
                try:
                    res_json = json.loads(res.text)
                except Exception as err:
                    logger.warn(f"{site} 签到返回解析失败，正在刷新验证码重试：{err}")
                    last_error = '签到失败：签到结果解析失败'
                    continue

                if res_json.get("success"):
                    logger.info(f"{site} 签到成功")
                    return True, '签到成功'
                if str(res_json.get("message")) == "date_unmatch":
                    logger.warn(f"{site} 重复成功")
                    return True, '今日已签到'
                if str(res_json.get("message")) == "invalid_imagehash":
                    logger.warn(f"{site} 签到失败：验证码错误，正在刷新重试")
                    last_error = '签到失败：验证码错误'
                    continue

                logger.warn(f"{site} 签到失败：签到接口返回异常，正在刷新重试：{res_json}")
                last_error = '签到失败：签到接口返回异常'
                continue

            logger.warn(f"{site} 签到失败：签到接口无响应，正在刷新重试")
            last_error = '签到失败：签到接口无响应'

        logger.error(f'{site} {last_error}')
        return False, last_error

import json
import time
from typing import Tuple

from lxml import etree
from ruamel.yaml import CommentedMap

from app.core.config import settings
from app.log import logger
from app.plugins.autosignin.captcha import CaptchaSolver
from app.plugins.autosignin.sites import _ISiteSigninHandler
from app.utils.http import RequestUtils
from app.utils.string import StringUtils


class Opencd(_ISiteSigninHandler):
    """
    OpenCD OCR sign-in.
    """

    site_url = "open.cd"
    _repeat_text = "/plugin_sign-in.php?cmd=show-log"

    @classmethod
    def match(cls, url: str) -> bool:
        return True if StringUtils.url_equal(url, cls.site_url) else False

    def signin(self, site_info: CommentedMap) -> Tuple[bool, str]:
        site = site_info.get("name")
        site_cookie = site_info.get("cookie")
        ua = site_info.get("ua")
        proxy = site_info.get("proxy")
        render = site_info.get("render")
        timeout = site_info.get("timeout")

        html_text = self.get_page_source(url='https://www.open.cd',
                                         cookie=site_cookie,
                                         ua=ua,
                                         proxy=proxy,
                                         render=render,
                                         timeout=timeout)
        if not html_text:
            logger.error(f"{site} 签到失败，请检查站点连通性")
            return False, '签到失败，请检查站点连通性'

        if "login.php" in html_text:
            logger.error(f"{site} 签到失败，Cookie已失效")
            return False, '签到失败，Cookie已失效'

        if self._repeat_text in html_text:
            logger.info(f"{site} 今日已签到")
            return True, '今日已签到'

        last_error = '签到失败：未获取到验证码'

        def _fetch_captcha():
            html_text = self.get_page_source(url='https://www.open.cd/plugin_sign-in.php',
                                             cookie=site_cookie,
                                             ua=ua,
                                             proxy=proxy,
                                             render=render,
                                             timeout=timeout)
            if not html_text:
                logger.error(f"{site} 签到失败，请检查站点连通性")
                return None, None

            html = etree.HTML(html_text)
            if html is None:
                return None, None

            try:
                img_url = html.xpath('//form[@id="frmSignin"]//img/@src')[0]
                img_hash = html.xpath('//form[@id="frmSignin"]//input[@name="imagehash"]/@value')[0]
            except IndexError:
                logger.error(f"{site} 签到失败，获取签到参数失败")
                return None, None

            if not img_url or not img_hash:
                logger.error(f"{site} 签到失败，获取签到参数失败")
                return None, None

            img_get_url = 'https://www.open.cd/%s' % img_url
            logger.debug(f"{site} 获取到验证码链接 {img_get_url}")
            return img_hash, img_get_url

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
                                                 website_url="https://www.open.cd/plugin_sign-in.php")
                logger.debug(f"ocr识别{site}验证码 {ocr_result}")
                if ocr_result and len(ocr_result) == 6:
                    logger.info(f"ocr识别{site}验证码成功 {ocr_result}")
                    break
                if times < 1:
                    logger.debug(f"ocr识别{site}验证码失败，正在进行重试，目前重试次数 {times + 1}")
                    time.sleep(1)

            if not ocr_result or len(ocr_result) != 6:
                last_error = '签到失败：验证码识别失败'
                continue

            data = {
                'imagehash': img_hash,
                'imagestring': ocr_result
            }
            sign_res = RequestUtils(cookies=site_cookie,
                                    ua=ua,
                                    proxies=settings.PROXY if proxy else None,
                                    timeout=timeout
                                    ).post_res(url='https://www.open.cd/plugin_sign-in.php?cmd=signin', data=data)
            if sign_res and sign_res.status_code == 200:
                logger.debug(f"sign_res返回 {sign_res.text}")
                try:
                    sign_dict = json.loads(sign_res.text)
                except Exception as err:
                    logger.error(f"{site} 签到失败，签到接口返回解析失败：{err}")
                    last_error = '签到失败：签到接口返回解析失败'
                    continue

                if sign_dict.get('state'):
                    logger.info(f"{site} 签到成功")
                    return True, '签到成功'

                logger.error(f"{site} 签到失败，签到接口返回{sign_dict}，正在刷新验证码重试")
                last_error = '签到失败'
                continue

            logger.error(f"{site} 签到失败，签到接口无响应，正在刷新验证码重试")
            last_error = '签到失败：签到接口无响应'

        logger.error(f"{site} {last_error}")
        return False, last_error
